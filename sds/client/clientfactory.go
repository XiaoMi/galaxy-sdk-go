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
				return net.DialTimeout(network, addr, soTimeout)
			},
			MaxIdleConnsPerHost: 10 * runtime.NumCPU(),
		},
	}
	return ClientFactory{credential: credential, httpClient: httpClient, agent: agent}
}

func (cf *ClientFactory) SetHTTPClient(client *http.Client) {
	cf.httpClient = client
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
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getServerVersion")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetServerVersion()
}

func (p *AdminClientProxy) ValidateClientVersion(clientVersion *common.Version) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=validateClientVersion")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.ValidateClientVersion(clientVersion)
}

func (p *AdminClientProxy) GetServerTime() (r int64, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getServerTime")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetServerTime()
}

func (p *AdminClientProxy) SaveAppInfo(appInfo *admin.AppInfo) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=saveAppInfo")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.SaveAppInfo(appInfo)
}

func (p *AdminClientProxy) GetAppInfo(appId string) (r *admin.AppInfo, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getAppInfo")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetAppInfo(appId)
}

func (p *AdminClientProxy) FindAllApps() (r []*admin.AppInfo, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=findAllApps")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.FindAllApps()
}

func (p *AdminClientProxy) FindAllTables() (r []*table.TableInfo, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=findAllTables")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.FindAllTables()
}

func (p *AdminClientProxy) CreateTable(tableName string,
	tableSpec *table.TableSpec) (r *table.TableInfo, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=createTable")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.CreateTable(tableName, tableSpec)
}

func (p *AdminClientProxy) DropTable(tableName string) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=dropTable")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.DropTable(tableName)
}

func (p *AdminClientProxy) AlterTable(tableName string, tableSpec *table.TableSpec) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=alterTable")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.AlterTable(tableName, tableSpec)
}

func (p *AdminClientProxy) CloneTable(srcName string, destTable string,
	flushTable bool) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=cloneTable")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.CloneTable(srcName, destTable, flushTable)
}

func (p *AdminClientProxy) DisableTable(tableName string) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=disableTable")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.DisableTable(tableName)
}

func (p *AdminClientProxy) EnableTable(tableName string) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=enableTable")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.EnableTable(tableName)
}

func (p *AdminClientProxy) DescribeTable(tableName string) (r *table.TableSpec, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=describeTable")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.DescribeTable(tableName)
}

func (p *AdminClientProxy) GetTableStatus(tableName string) (r *table.TableStatus, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getTableStatus")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetTableStatus(tableName)
}

func (p *AdminClientProxy) GetTableState(tableName string) (r table.TableState, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getTableState")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetTableState(tableName)
}

func (p *AdminClientProxy) GetTableSplits(tableName string, startKey table.Dictionary,
	stopKey table.Dictionary) (r []*table.TableSplit, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getTableSplits")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetTableSplits(tableName, startKey, stopKey)
}

func (p *AdminClientProxy) QueryMetric(query *admin.MetricQueryRequest) (r *admin.TimeSeriesData,
	err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=queryMetric")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.QueryMetric(query)
}

func (p *AdminClientProxy) QueryMetrics(queries []*admin.MetricQueryRequest) (r []*admin.TimeSeriesData, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=queryMetrics")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.QueryMetrics(queries)
}

func (p *AdminClientProxy) FindAllAppInfo() (r []*admin.AppInfo, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=findAllAppInfo")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.FindAllAppInfo()
}

func (p *AdminClientProxy) GetTableSize(tableName string) (r int64, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getTableSize")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetTableSize(tableName)
}

func (p *AdminClientProxy) PutClientMetrics(clientMetrics *admin.ClientMetrics) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=pushClientMetrics")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.PutClientMetrics(clientMetrics)
}

func (p *AdminClientProxy) SubscribePhoneAlert(phoneNumber string, spaceId string) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=subscribePhoneAlert")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.SubscribePhoneAlert(phoneNumber, spaceId)
}

func (p *AdminClientProxy) UnsubscribePhoneAlert(phoneNumber string, spaceId string) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=unsubscribePhoneAlert")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.UnsubscribePhoneAlert(phoneNumber, spaceId)
}

func (p *AdminClientProxy) SubscribeEmailAlert(email string, spaceId string) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=subscribeEmailAlert")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.SubscribeEmailAlert(email, spaceId)
}

func (p *AdminClientProxy) UnsubscribeEmailAlert(email string, spaceId string) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=unsubscribeEmailAlert")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.UnsubscribeEmailAlert(email, spaceId)
}

func (p *AdminClientProxy) ListSubscribedPhone(spaceId string) (r []string, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=listSubscribedPhone")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.ListSubscribedPhone(spaceId)
}

func (p *AdminClientProxy) ListSubscribedEmail(spaceId string) (r []string, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=listSubscribedEmail")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.ListSubscribedEmail(spaceId)
}

func (p *AdminClientProxy) GetTableHistorySize(tableName string, startDate int64, stopDate int64) (r map[int64]int64, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getTableHistorySize")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetTableHistorySize(tableName, startDate, stopDate)
}

func (p *AdminClientProxy) RenameTable(srcName string, destName string) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=renameTable")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.RenameTable(srcName, destName)
}

func (p *AdminClientProxy) ListSnapshots(tableName string) (r *admin.TableSnapshots, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=listSnapshots")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.ListSnapshots(tableName)
}

func (p *AdminClientProxy) SnapshotTable(tableName string, snapshotName string) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=snapshotTable")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.SnapshotTable(tableName, snapshotName)
}


func (p *AdminClientProxy) DeleteSnapshot(tableName string, snapshotName string) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=deleteSnapshot")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.DeleteSnapshot(tableName, snapshotName)
}

func (p *AdminClientProxy) RestoreSnapshot(tableName string, snapshotName string, destTableName string, isSystem table.SnapshotType) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=restoreSnapshot")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.RestoreSnapshot(tableName, snapshotName, destTableName, isSystem)
}

func (p *AdminClientProxy) ListAllSnapshots(spaceId string) (r []*admin.SnapshotTableView, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=listAllSnapshots")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.ListAllSnapshots(spaceId)
}

func (p *AdminClientProxy) CancelSnapshotTable(tableName string, snapshotName string) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=cancelSnapshotTable")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.CancelSnapshotTable(tableName, snapshotName)
}

func (p *AdminClientProxy) GetSnapshotState(tableName string, snapshotName string) (r table.SnapshotState, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getSnapshotState")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetSnapshotState(tableName, snapshotName)
}

func (p *AdminClientProxy) GetQuotaInfo(spaceId string) (r *admin.QuotaInfo, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getSnapshotState")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetQuotaInfo(spaceId)
}

func (p *AdminClientProxy) GetCeilStreamCheckpoint(tableName string, topicName string, timestamp int64) (r *admin.StreamCheckpoint, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getSnapshotState")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetCeilStreamCheckpoint(tableName, topicName, timestamp)
}

func (p *AdminClientProxy) GetDefaultColdStandBy() (r *table.ColdStandBy, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getSnapshotState")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetDefaultColdStandBy()
}

func (p *AdminClientProxy) GetDevelopId() (r string, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getSnapshotState")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetDevelopId()
}

func (p *AdminClientProxy) GetFloorStreamCheckpoint(tableName string, topicName string, timestamp int64) (r *admin.StreamCheckpoint, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getSnapshotState")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetFloorStreamCheckpoint(tableName, topicName, timestamp)
}

func (p *AdminClientProxy) GetGrantRule(spaceId string) (r *admin.GrantRule, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getSnapshotState")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetGrantRule(spaceId)
}

func (p *AdminClientProxy) GetIndexTableSplits(tableName string, indexName string, startKey table.Dictionary, stopKey table.Dictionary) (r []*table.TableSplit, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getSnapshotState")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetIndexTableSplits(tableName, indexName, startKey, stopKey)
}

func (p *AdminClientProxy) SetGrantRule(spaceId string, grantRule *admin.GrantRule) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getSnapshotState")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.SetGrantRule(spaceId, grantRule)
}

func (p *AdminClientProxy) GetLatestStreamCheckpoint(tableName string, topicName string) (r *admin.StreamCheckpoint, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getSnapshotState")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetLatestStreamCheckpoint(tableName, topicName)
}


func (p *AdminClientProxy) ListAllDeletedTables(spaceId string) (r []*table.TableInfo, err error){
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getSnapshotState")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.ListAllDeletedTables(spaceId)
}

func (p *AdminClientProxy) RecoverTable(srcTableName string, destTableName string, topicName string, timestamp int64) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getSnapshotState")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.RecoverTable(srcTableName, destTableName, topicName, timestamp)
}

func (p *AdminClientProxy) RestoreTable(deletedTableName string, destTableName string) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getSnapshotState")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.RestoreTable(deletedTableName, destTableName)
}

func (p *AdminClientProxy) SetSpaceId(tableName string, spaceId string) (err error){
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getSnapshotState")
	defer trans.Close()
	client := admin.NewAdminServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.SetSpaceId(tableName, spaceId)
}


////////////////////////////
// Table client proxy
////////////////////////////
type TableClientProxy struct {
	factory     *SdsTHttpClientTransportFactory
	clockOffset int64
}

func (p *TableClientProxy) GetServerVersion() (r *common.Version, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getServerVersion")
	defer trans.Close()
	client := table.NewTableServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetServerVersion()
}

func (p *TableClientProxy) ValidateClientVersion(clientVersion *common.Version) (err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=validateClientVersion")
	defer trans.Close()
	client := table.NewTableServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.ValidateClientVersion(clientVersion)
}

func (p *TableClientProxy) GetServerTime() (r int64, err error) {
	trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, "type=getServerTime")
	defer trans.Close()
	client := table.NewTableServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
	return client.GetServerTime()
}

func (p *TableClientProxy) Get(request *table.GetRequest) (r *table.GetResult_, err error) {
	for retry := 0; retry <= errors.MAX_RETRY; {
		query := fmt.Sprintf("type=get&name=%s", request.GetTableName())
		trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, query)
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
		query := fmt.Sprintf("type=put&name=%s", request.GetTableName())
		trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, query)
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
		query := fmt.Sprintf("type=increment&name=%s", request.GetTableName())
		trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, query)
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
		query := fmt.Sprintf("type=remove&name=%s", request.GetTableName())
		trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, query)
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

func (p *TableClientProxy) RealRemove(request []*table.RemoveRequest) (r []*table.RemoveResult_,
	err error) {
	for retry := 0; retry <= errors.MAX_RETRY; {
		query := fmt.Sprintf("type=realRemove&name=%s", request[0].GetTableName())
		trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, query)
		defer trans.Close()
		client := table.NewTableServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
		if r, e := client.RealRemove(request); e != nil {
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
		query := fmt.Sprintf("type=scan&name=%s", request.GetTableName())
		trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, query)
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
		action := request.GetItems()[0].GetAction()
		rq := request.GetItems()[0].GetRequest()
		var tableName string
		switch(action) {
		case table.BatchOp_GET:
			tableName = rq.GetGetRequest().GetTableName()
			break
		case table.BatchOp_PUT:
			tableName = rq.GetPutRequest().GetTableName()
			break
		case table.BatchOp_INCREMENT:
			tableName = rq.GetIncrementRequest().GetTableName()
			break
		case table.BatchOp_REMOVE:
			tableName = rq.GetRemoveRequest().GetTableName()
			break
		}
		query := fmt.Sprintf("type=batch&name=%s", tableName)
		trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, query)
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

func (p *TableClientProxy) PartialAllowedBatch(request *table.BatchRequest) (r *table.BatchResult_, err error) {
	for retry := 0; retry <= errors.MAX_RETRY; {
		action := request.GetItems()[0].GetAction()
		rq := request.GetItems()[0].GetRequest()
		var tableName string
		switch(action) {
		case table.BatchOp_GET:
			tableName = rq.GetGetRequest().GetTableName()
			break
		case table.BatchOp_PUT:
			tableName = rq.GetPutRequest().GetTableName()
			break
		case table.BatchOp_INCREMENT:
			tableName = rq.GetIncrementRequest().GetTableName()
			break
		case table.BatchOp_REMOVE:
			tableName = rq.GetRemoveRequest().GetTableName()
			break
		}
		query := fmt.Sprintf("type=batch&name=%s", tableName)
		trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, query)
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



func (p *TableClientProxy) BatchCheckAndMutate(request *table.BatchRequest) (r *table.BatchResult_, err error) {
	for retry := 0; retry <= errors.MAX_RETRY; {
		action := request.GetItems()[0].GetAction()
		rq := request.GetItems()[0].GetRequest()
		var tableName string
		switch(action) {
		case table.BatchOp_GET:
			tableName = rq.GetGetRequest().GetTableName()
			break
		case table.BatchOp_PUT:
			tableName = rq.GetPutRequest().GetTableName()
			break
		case table.BatchOp_INCREMENT:
			tableName = rq.GetIncrementRequest().GetTableName()
			break
		case table.BatchOp_REMOVE:
			tableName = rq.GetRemoveRequest().GetTableName()
			break
		}
		query := fmt.Sprintf("type=batchCheckAndMutate&name=%s", tableName)
		trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, query)
		defer trans.Close()
		client := table.NewTableServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
		if r, e := client.BatchCheckAndMutate(request); e != nil {
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
func (p *TableClientProxy) PutToRebuildIndex(request *table.PutRequest) (r *table.PutResult_, err error) {
	for retry := 0; retry <= errors.MAX_RETRY; {
		query := fmt.Sprintf("type=putToRebuild&name=%s", request.GetTableName())
		trans := p.factory.GetTransportWithClockOffset(nil, p.clockOffset, query)
		defer trans.Close()
		client := table.NewTableServiceClientFactory(trans, thrift.NewTJSONProtocolFactory())
		if r, e := client.PutToRebuildIndex(request); e != nil {
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
