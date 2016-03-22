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
	"fmt"
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"io"
	"net/http"
	"net/url"
	"strings"
	"strconv"
	"time"
	"github.com/golang/glog"
	"github.com/nu7hatch/gouuid"
	"github.com/XiaoMi/galaxy-sdk-go/thrift"
	"github.com/XiaoMi/galaxy-sdk-go/sds/auth"
	"github.com/XiaoMi/galaxy-sdk-go/sds/errors"
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
	queryString	  string
}

type SdsTHttpClientTransportFactory struct {
	credential  *auth.Credential
	url         string
	agent       string
	httpClient  *http.Client
}

func NewTHttpClientTransportFactory(url string, credential *auth.Credential,
	httpClient *http.Client, agent string) *SdsTHttpClientTransportFactory {
	return &SdsTHttpClientTransportFactory {
		credential: credential,
		url:        url,
		agent:      agent,
		httpClient: httpClient,
	}
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
			return s
		}
	}
	s, _ := newSdsTHttpClient(p.url, p.credential, p.httpClient, p.agent, clockOffset, query)
	return s
}

func newSdsTHttpClient(urlstr string, credential *auth.Credential, httpClient *http.Client,
	agent string, clockOffset int64, queryString string) (thrift.TTransport, error) {
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
	return requestId.String()[0 : length]
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
	for k, v := range *p.createHeaders() {
		glog.V(2).Infof("%s: %s", k, v)
		p.header.Add(k, v)
	}
	req.Header = p.header
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

func (p *SdsTHttpClient) createHeaders() *map[string]string {
	var _ = sha1.Size
	headers := make(map[string]string)
	headers[auth.HK_HOST] = p.url.Host
	headers[auth.HK_TIMESTAMP] = fmt.Sprintf("%d", time.Now().Unix() + p.clockOffset)
	md5c := md5.New()
	io.WriteString(md5c, p.requestBuffer.String())
	headers[auth.HK_CONTENT_MD5] = fmt.Sprintf("%x", md5c.Sum(nil))

	authHeader := auth.NewHttpAuthorizationHeader()
	authHeader.Algorithm = auth.MacAlgorithmPtr(auth.MacAlgorithm_HmacSHA1)
	authHeader.UserType = p.credential.GetTypeA1()
	authHeader.SecretKeyId = p.credential.SecretKeyId

	signedHeaders := make([]string, 0, len(headers))
	signedValues := make([]string, 0, len(headers))
	for k, v := range headers {
		signedHeaders = append(signedHeaders, k)
		signedValues = append(signedValues, v)
	}
	rawstr := strings.Join(signedValues, "\n")
	mac := hmac.New(sha1.New, []byte(*p.credential.SecretKey))
	mac.Write([]byte(rawstr))
	authHeader.Signature = thrift.StringPtr(fmt.Sprintf("%x", mac.Sum(nil)))
	authHeader.SignedHeaders = &signedHeaders
	buff := thrift.NewTMemoryBuffer()
	proto := thrift.NewTJSONProtocol(buff)
	authHeader.Write(proto)
	proto.Flush()
	headers[auth.HK_AUTHORIZATION] = buff.String()

	headers["Host"] = p.url.Host
	headers["Content-Type"] = "application/x-thrift"
	headers["Content-Length"] = strconv.Itoa(p.requestBuffer.Len())
	headers["User-Agent"] = p.agent

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
