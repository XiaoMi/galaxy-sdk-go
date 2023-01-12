/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package client

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/XiaoMi/galaxy-sdk-go/sds/auth"
	"github.com/XiaoMi/galaxy-sdk-go/sds/common"
	"github.com/XiaoMi/galaxy-sdk-go/sds/errors"
	"github.com/XiaoMi/galaxy-sdk-go/thrift"
	"github.com/golang/glog"
	"github.com/nu7hatch/gouuid"
)

type SdsTHttpClient struct {
	credential    *auth.Credential
	agent         string
	response      *http.Response
	url           *url.URL
	requestBuffer *bytes.Buffer
	header        http.Header
	clockOffset   int64
	httpClient    *http.Client
	queryString   string
	tProtocol     *common.ThriftProtocol
}

type SdsTHttpClientTransportFactory struct {
	credential *auth.Credential
	url        string
	agent      string
	httpClient *http.Client
	tProtocol  *common.ThriftProtocol
}

func NewTHttpClientTransportFactory(url string, credential *auth.Credential,
	httpClient *http.Client, agent string) *SdsTHttpClientTransportFactory {
	return &SdsTHttpClientTransportFactory{
		credential: credential,
		url:        url,
		agent:      agent,
		httpClient: httpClient,
	}
}

// SetTProtocol Set the thrift protocol for Thrift Transport factory
// It decides the "Content-Type" in http request header, which specify server parse protocol
func (p *SdsTHttpClientTransportFactory) SetTProtocol(tProtocol common.ThriftProtocol) {
	p.tProtocol = &tProtocol
}

func (p *SdsTHttpClientTransportFactory) GetTransport(trans thrift.TTransport) thrift.TTransport {
	return p.GetTransportWithClockOffset(trans, 0, "")
}

func (p *SdsTHttpClientTransportFactory) GetTransportWithClockOffset(trans thrift.TTransport,
	clockOffset int64, query string) thrift.TTransport {
	if trans != nil {
		t, ok := trans.(*SdsTHttpClient)
		if ok && t.url != nil {
			s, _ := newSdsTHttpClient(t.url.String(), t.credential, t.httpClient,
				t.agent, t.clockOffset, query)
			s.SetTProtocol(t.tProtocol)
			return s
		}
	}
	s, _ := newSdsTHttpClient(p.url, p.credential, p.httpClient, p.agent, clockOffset, query)
	s.SetTProtocol(p.tProtocol)
	return s
}

func newSdsTHttpClient(urlstr string, credential *auth.Credential, httpClient *http.Client,
	agent string, clockOffset int64, queryString string) (*SdsTHttpClient, error) {
	parsedURL, err := url.Parse(urlstr)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 1024)
	return &SdsTHttpClient{
		credential:    credential,
		agent:         agent,
		url:           parsedURL,
		requestBuffer: bytes.NewBuffer(buf),
		header:        http.Header{},
		httpClient:    httpClient,
		clockOffset:   clockOffset,
		queryString:   queryString,
	}, nil
}

// SetTProtocol Set the thrift protocol for Thrift Transport factory
// It decides the "Content-Type" in http request header, which specify server parse protocol
func (p *SdsTHttpClient) SetTProtocol(tProtocol *common.ThriftProtocol) {
	p.tProtocol = tProtocol
}

// Set the HTTP Header for this specific Thrift Transport
// It is important that you first assert the TTransport as a SdsTHttpClient type
// like so:
//
// httpTrans := trans.(SdsTHttpClient)
// httpTrans.SetHeader("User-Agent","Thrift Client 1.0")
func (p *SdsTHttpClient) SetHeader(key string, value string) {
	p.header.Add(key, value)
}

// Get the HTTP Header represented by the supplied Header Key for this specific Thrift Transport
// It is important that you first assert the TTransport as a SdsTHttpClient type
// like so:
//
// httpTrans := trans.(SdsTHttpClient)
// hdrValue := httpTrans.GetHeader("User-Agent")
func (p *SdsTHttpClient) GetHeader(key string) string {
	return p.header.Get(key)
}

// Deletes the HTTP Header given a Header Key for this specific Thrift Transport
// It is important that you first assert the TTransport as a SdsTHttpClient type
// like so:
//
// httpTrans := trans.(SdsTHttpClient)
// httpTrans.DelHeader("User-Agent")
func (p *SdsTHttpClient) DelHeader(key string) {
	p.header.Del(key)
}

func (p *SdsTHttpClient) Open() error {
	// do nothing
	return nil
}

func (p *SdsTHttpClient) IsOpen() bool {
	return p.response != nil || p.requestBuffer != nil
}

func (p *SdsTHttpClient) Peek() bool {
	return p.IsOpen()
}

func (p *SdsTHttpClient) Close() error {
	if p.requestBuffer != nil {
		p.requestBuffer.Reset()
		p.requestBuffer = nil
	}
	p.header = http.Header{}
	if p.response != nil && p.response.Body != nil {
		err := p.response.Body.Close()
		p.response = nil
		return err
	}
	return nil
}

func (p *SdsTHttpClient) Read(buf []byte) (int, error) {
	if p.response == nil {
		return 0, thrift.NewTTransportException(thrift.NOT_OPEN, "Response buffer is empty, no request.")
	}
	n, err := p.response.Body.Read(buf)
	if n > 0 && (err == nil || err == io.EOF) {
		glog.V(2).Infof("read: %s", string(buf))
		return n, nil
	}
	return n, thrift.NewTTransportExceptionFromError(err)
}

func (p *SdsTHttpClient) ReadByte() (c byte, err error) {
	return readByte(p.response.Body)
}

func (p *SdsTHttpClient) Write(buf []byte) (int, error) {
	n, err := p.requestBuffer.Write(buf)
	return n, err
}

func (p *SdsTHttpClient) WriteByte(c byte) error {
	return p.requestBuffer.WriteByte(c)
}

func (p *SdsTHttpClient) WriteString(s string) (n int, err error) {
	return p.requestBuffer.WriteString(s)
}

func (p *SdsTHttpClient) generateRandomId(length int) string {
	requestId, _ := uuid.NewV4()
	return requestId.String()[0:length]
}

func (p *SdsTHttpClient) Flush() error {
	requestId := p.generateRandomId(8)
	var uri string
	if p.queryString == "" {
		uri = fmt.Sprintf("%s?id=%s", p.url.String(), requestId)
	} else {
		uri = fmt.Sprintf("%s?id=%s&%s", p.url.String(), requestId, p.queryString)
	}
	req, err := http.NewRequest("POST", uri, p.requestBuffer)
	if err != nil {
		return thrift.NewTTransportExceptionFromError(err)
	}
	canonicalizeResource := p.canonicalizeResource(uri)

	for k, v := range *p.createHeaders() {
		glog.V(2).Infof("%s: %s", k, v)
		p.header.Add(k, v)
	}

	req.Header = p.header
	req.Header.Add(auth.HK_AUTHORIZATION, p.authHeaders(&req.Header, canonicalizeResource))
	glog.V(2).Infof("Send http request: %s\n", p.requestBuffer)
	response, err := p.httpClient.Do(req)
	if err != nil {
		glog.Errorf("Failed to exec http request: %v\n", req)
		return thrift.NewTTransportExceptionFromError(err)
	}
	p.response = response
	if response.StatusCode != http.StatusOK {
		var serverTime int64
		hts := response.Header.Get(auth.HK_TIMESTAMP)
		if ts, err := strconv.Atoi(hts); err == nil {
			serverTime = int64(ts)
		} else {
			serverTime = time.Now().Unix()
		}
		glog.Errorf("HTTP status: %s, failed to exec http request: %v\n", response.Status, req)
		return NewSdsTransportError(errors.HttpStatusCode(int64(response.StatusCode)),
			response.Status, serverTime)
	}
	return nil
}

func (p *SdsTHttpClient) canonicalizeResource(uri string) string {
	subResource := []string{"acl", "quota", "uploads", "partNumber", "uploadId",
		"storageAccessToken", "metadata"}
	parseUrl, _ := url.Parse(uri)
	result := parseUrl.Path
	queryArgs := parseUrl.Query()
	canonicalizeQuery := make([]string, 0, len(queryArgs))
	for k, _ := range queryArgs {
		if p.contains(&subResource, k) {
			canonicalizeQuery = append(canonicalizeQuery, k)
		}
	}
	if len(canonicalizeQuery) != 0 {
		i := 0
		sort.Strings(canonicalizeQuery)
		for _, v := range canonicalizeQuery {
			if i == 0 {
				result = fmt.Sprintf("%s?", result)
			} else {
				result = fmt.Sprintf("%s&", result)
			}
			values := queryArgs[v]
			if len(values) == 1 && values[0] == "" {
				result = fmt.Sprintf("%s%s", result, v)
			} else {
				result = fmt.Sprintf("%s%s=%s", result, v, values[len(values)-1])
			}
			i++
		}
	}
	return result
}

func (p *SdsTHttpClient) contains(arr *[]string, target string) bool {
	for _, v := range *arr {
		if strings.EqualFold(v, target) {
			return true
		}
	}
	return false
}

func (p *SdsTHttpClient) getDate() string {
	t := time.Now()
	timeStr := t.UTC().Format(time.RFC1123)
	return strings.Replace(timeStr, "UTC", "GMT", -1)
}

func (p *SdsTHttpClient) authHeaders(headers *http.Header, canonicalizeResource string) string {
	stringToSign := "POST\n"
	stringToSign = fmt.Sprintf("%s%s\n", stringToSign, p.getHeader(headers, "content-md5"))
	stringToSign = fmt.Sprintf("%s%s\n\n", stringToSign, p.getHeader(headers, "content-type"))
	stringToSign = fmt.Sprintf("%s%s", stringToSign, p.canonicalizeXiaomiHeaders(headers))
	stringToSign = fmt.Sprintf("%s%s", stringToSign, canonicalizeResource)
	mac := hmac.New(sha1.New, []byte(*p.credential.SecretKey))
	mac.Write([]byte(stringToSign))
	return fmt.Sprintf("Galaxy-V3 %s:%s", *p.credential.SecretKeyId, base64.StdEncoding.EncodeToString(mac.Sum(nil)))
}

func (p *SdsTHttpClient) canonicalizeXiaomiHeaders(headers *http.Header) string {
	canonicalizedKeys := make([]string, 0, len(*headers))
	canonicalizedHeaders := make(map[string]string)
	for k, v := range *headers {
		lowerKey := strings.ToLower(k)
		if strings.Index(lowerKey, "x-xiaomi-") == 0 {
			canonicalizedKeys = append(canonicalizedKeys, lowerKey)
			canonicalizedHeaders[lowerKey] = strings.Join(v, ",")
		}
	}
	sort.Strings(canonicalizedKeys)
	result := ""
	for i := range canonicalizedKeys {
		result = fmt.Sprintf("%s%s:%s\n", result, canonicalizedKeys[i],
			canonicalizedHeaders[canonicalizedKeys[i]])
	}
	return result
}

func (p *SdsTHttpClient) getHeader(headers *http.Header, key string) string {
	for k, v := range *headers {
		lowerKey := strings.ToLower(k)
		if strings.EqualFold(key, lowerKey) {
			return v[0]
		}
	}
	return ""
}

func (p *SdsTHttpClient) createHeaders() *map[string]string {
	var _ = sha1.Size
	headers := make(map[string]string)
	headers[auth.HK_HOST] = p.url.Host
	headers[auth.HK_TIMESTAMP] = fmt.Sprintf("%d", time.Now().Unix()+p.clockOffset)
	md5c := md5.New()
	io.WriteString(md5c, p.requestBuffer.String())
	headers[auth.HK_CONTENT_MD5] = fmt.Sprintf("%x", md5c.Sum(nil))
	headers[auth.MI_DATE] = p.getDate()
	headers["Content-Length"] = strconv.Itoa(p.requestBuffer.Len())
	headers["User-Agent"] = p.agent

	contentType := "application/x-thrift"
	if p.tProtocol != nil {
		contentType = common.THRIFT_HEADER_MAP[*p.tProtocol]
	}
	headers["Content-Type"] = contentType

	return &headers
}

func readByte(r io.Reader) (c byte, err error) {
	v := [1]byte{0}
	n, err := r.Read(v[0:1])
	if n > 0 && (err == nil || err == io.EOF) {
		return v[0], nil
	}
	if n > 0 && err != nil {
		return v[0], err
	}
	if err != nil {
		return 0, err
	}
	return v[0], nil
}
