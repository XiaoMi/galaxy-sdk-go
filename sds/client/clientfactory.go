package client

import (
	"fmt"
	"net/http"
	"net"
	"runtime"
	"time"
	"github.com/golang/glog"
	"github.com/XiaoMi/galaxy-sdk-go/sds/admin"
	"github.com/XiaoMi/galaxy-sdk-go/sds/auth"
	"github.com/XiaoMi/galaxy-sdk-go/sds/common"
	"github.com/XiaoMi/galaxy-sdk-go/sds/table"
	"github.com/XiaoMi/galaxy-sdk-go/thrift"
	"github.com/XiaoMi/galaxy-sdk-go/sds/errors"
)

type ClientFactory struct {
	credential *auth.Credential
	httpClient *http.Client
	agent      string
}

func NewClientFactory(credential *auth.Credential, soTimeout time.Duration) ClientFactory {
	ver := common.NewVersion()
	verstr := fmt.Sprintf("%d.%d.%s", ver.Major, ver.Minor, ver.Patch)
	agent := fmt.Sprintf("Go-SDK/%s Go/%s-%s-%s",
		verstr, runtime.GOOS, runtime.GOARCH, runtime.Version())
	httpClient := &http.Client{
		Transport: &http.Transport{
			Dial: func(network, addr string) (net.Conn, error) {
				return net.DialTimeout(network, addr, soTimeout);
			},
		},
	}
	return ClientFactory{credential: credential, httpClient: httpClient, agent: agent}
}

func (cf *ClientFactory) NewDefaultAdminClient() (admin.AdminService) {
	return cf.NewAdminClient(common.DEFAULT_SERVICE_ENDPOINT+common.ADMIN_SERVICE_PATH)
}

func (cf *ClientFactory) NewDefaultSecureAdminClient() (admin.AdminService) {
	return cf.NewAdminClient(common.DEFAULT_SECURE_SERVICE_ENDPOINT+common.ADMIN_SERVICE_PATH)
}

func (cf *ClientFactory) NewAdminClient(url string) (admin.AdminService) {
	transFactory := NewTHttpClientTransportFactory(url, cf.credential, cf.httpClient, cf.agent)
	return &AdminClientProxy{factory: transFactory, clockOffset: 0}
}

func (cf *ClientFactory) NewDefaultTableClient() (table.TableService) {
	return cf.NewTableClient(common.DEFAULT_SERVICE_ENDPOINT+common.TABLE_SERVICE_PATH)
}

func (cf *ClientFactory) NewDefaultSecureTableClient() (table.TableService) {
	return cf.NewTableClient(common.DEFAULT_SECURE_SERVICE_ENDPOINT+common.TABLE_SERVICE_PATH)
}

func (cf *ClientFactory) NewTableClient(url string) (table.TableService) {
	transFactory := NewTHttpClientTransportFactory(url, cf.credential, cf.httpClient, cf.agent)
	return &TableClientProxy{factory: transFactory, clockOffset: 0}
}

////////////////////////////
// Admin client proxy
////////////////////////////
type AdminClientProxy struct {
	factory     *SdsTHttpClientTransportFactory
	clockOffset int64
}

func (p *AdminClientProxy) GetServerVersion() (r *common.Version, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetServerVersion()
}

func (p *AdminClientProxy) ValidateClientVersion(clientVersion *common.Version) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.ValidateClientVersion(clientVersion)
}

func (p *AdminClientProxy) GetServerTime() (r int64, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetServerTime()
}

func (p *AdminClientProxy) SaveAppInfo(appInfo *admin.AppInfo) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.SaveAppInfo(appInfo)
}

func (p *AdminClientProxy) GetAppInfo(appId string) (r *admin.AppInfo, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetAppInfo(appId)
}

func (p *AdminClientProxy) FindAllApps() (r []*admin.AppInfo, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.FindAllApps()
}

func (p *AdminClientProxy) FindAllTables() (r []*table.TableInfo, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.FindAllTables()
}

func (p *AdminClientProxy) CreateTable(tableName string,
	tableSpec *table.TableSpec) (r *table.TableInfo, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.CreateTable(tableName, tableSpec)
}

func (p *AdminClientProxy) DropTable(tableName string) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.DropTable(tableName)
}

func (p *AdminClientProxy) LazyDropTable(tableName string) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.LazyDropTable(tableName)
}

func (p *AdminClientProxy) AlterTable(tableName string, tableSpec *table.TableSpec) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.AlterTable(tableName, tableSpec)
}

func (p *AdminClientProxy) CloneTable(srcName string, destTable string,
	flushTable bool) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.CloneTable(srcName, destTable, flushTable)
}

func (p *AdminClientProxy) DisableTable(tableName string) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.DisableTable(tableName)
}

func (p *AdminClientProxy) EnableTable(tableName string) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.EnableTable(tableName)
}

func (p *AdminClientProxy) DescribeTable(tableName string) (r *table.TableSpec, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.DescribeTable(tableName)
}

func (p *AdminClientProxy) GetTableStatus(tableName string) (r *table.TableStatus, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetTableStatus(tableName)
}

func (p *AdminClientProxy) GetTableState(tableName string) (r table.TableState, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetTableState(tableName)
}

func (p *AdminClientProxy) GetTableSplits(tableName string, startKey table.Dictionary,
	stopKey table.Dictionary) (r []*table.TableSplit, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetTableSplits(tableName, startKey, stopKey)
}

func (p *AdminClientProxy) QueryMetric(query *admin.MetricQueryRequest) (r *admin.TimeSeriesData,
	err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.QueryMetric(query)
}

func (p *AdminClientProxy) QueryMetrics(queries []*admin.MetricQueryRequest) (r []*admin.TimeSeriesData, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.QueryMetrics(queries)
}

////////////////////////////
// Table client proxy
////////////////////////////
type TableClientProxy struct {
	factory     *SdsTHttpClientTransportFactory
	clockOffset int64
}

func (p *TableClientProxy) GetServerVersion() (r *common.Version, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := table.NewTableServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetServerVersion()
}

func (p *TableClientProxy) ValidateClientVersion(clientVersion *common.Version) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := table.NewTableServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.ValidateClientVersion(clientVersion)
}

func (p *TableClientProxy) GetServerTime() (r int64, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
	defer trans.Close()
	client := table.NewTableServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetServerTime()
}

func (p *TableClientProxy) Get(request *table.GetRequest) (r *table.GetResult_, err error) {
	for retry := 0; retry <= errors.MAX_RETRY; {
		trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
		defer trans.Close()
		client := table.NewTableServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
		if r, e := client.Get(request); e != nil {
			if p.shouldRetry(e) {
				err = e
				retry += 1
				continue
			}
			return r, e
		} else {
			return r, e
		}
	}
	return nil, err
}

func (p *TableClientProxy) Put(request *table.PutRequest) (r *table.PutResult_, err error) {
	for retry := 0; retry <= errors.MAX_RETRY; {
		trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
		defer trans.Close()
		client := table.NewTableServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
		if r, e := client.Put(request); e != nil {
			if p.shouldRetry(e) {
				err = e
				retry += 1
				continue
			}
			return r, e
		} else {
			return r, e
		}
	}
	return nil, err
}

func (p *TableClientProxy) Increment(request *table.IncrementRequest) (r *table.IncrementResult_,
	err error) {
	for retry := 0; retry <= errors.MAX_RETRY; {
		trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
		defer trans.Close()
		client := table.NewTableServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
		if r, e := client.Increment(request); e != nil {
			if p.shouldRetry(e) {
				err = e
				retry += 1
				continue
			}
			return r, e
		} else {
			return r, e
		}
	}
	return nil, err
}

func (p *TableClientProxy) Remove(request *table.RemoveRequest) (r *table.RemoveResult_,
	err error) {
	for retry := 0; retry <= errors.MAX_RETRY; {
		trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
		defer trans.Close()
		client := table.NewTableServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
		if r, e := client.Remove(request); e != nil {
			if p.shouldRetry(e) {
				err = e
				retry += 1
				continue
			}
			return r, e
		} else {
			return r, e
		}
	}
	return nil, err
}

func (p *TableClientProxy) Scan(request *table.ScanRequest) (r *table.ScanResult_, err error) {
	for retry := 0; retry <= errors.MAX_RETRY; {
		trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
		defer trans.Close()
		client := table.NewTableServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
		if r, e := client.Scan(request); e != nil {
			if p.shouldRetry(e) {
				err = e
				retry += 1
				continue
			}
			return r, e
		} else {
			return r, e
		}
	}
	return nil, err
}

func (p *TableClientProxy) Batch(request *table.BatchRequest) (r *table.BatchResult_, err error) {
	for retry := 0; retry <= errors.MAX_RETRY; {
		trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset)
		defer trans.Close()
		client := table.NewTableServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
		if r, e := client.Batch(request); e != nil {
			if p.shouldRetry(e) {
				err = e
				retry += 1
				continue
			}
			return r, e
		} else {
			return r, e
		}
	}
	return nil, err
}

func (p *TableClientProxy) shouldRetry(err error) bool {
	if se, ok := err.(SdsErrorCodePeeker); ok {
		if se.GetErrorCode() == errors.ErrorCode_CLOCK_TOO_SKEWED {
			if te, ok := err.(*SdsTransportError); ok {
				p.clockOffset = te.ServerTime-time.Now().Unix()
				glog.V(1).Infof("Adjusting local clock with offset: %d", p.clockOffset)
			}
		}
		if backoff, ok := errors.ERROR_BACKOFF[se.GetErrorCode()]; ok && backoff > 0 {
			duration := time.Duration(int64(backoff) * int64(time.Millisecond))
			glog.Infof("Backoff with %s and retry due to error %s", duration, se.GetErrorCode())
			time.Sleep(duration)
			return true
		}
	}
	return false
}
