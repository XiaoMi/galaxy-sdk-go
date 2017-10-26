package client

import (
	"fmt"
	"net/http"
	"net"
	"runtime"
	"time"
	"github.com/golang/glog"
	"github.com/XiaoMi/galaxy-sdk-go/emq/message"
	"github.com/XiaoMi/galaxy-sdk-go/emq/queue"
	"github.com/XiaoMi/galaxy-sdk-go/emq/common"
	"github.com/XiaoMi/galaxy-sdk-go/emq/constants"
	"github.com/XiaoMi/galaxy-sdk-go/rpc/auth"
	"github.com/XiaoMi/galaxy-sdk-go/thrift"
)

type ClientFactory struct {
	credential *auth.Credential
	httpClient *http.Client
	agent      string
}

func NewClientFactory(credential *auth.Credential, soTimeout time.Duration) ClientFactory {
	ver := common.NewVersion()
	verstr := fmt.Sprintf("%d.%d", ver.Major, ver.Minor)
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

func (cf *ClientFactory) NewDefaultQueueClient() (queue.QueueService) {
	return cf.NewQueueClient(constants.DEFAULT_SERVICE_ENDPOINT + constants.QUEUE_SERVICE_PATH)
}

func (cf *ClientFactory) NewDefaultSecureQueueClient() (queue.QueueService) {
	return cf.NewQueueClient(constants.DEFAULT_SECURE_SERVICE_ENDPOINT + constants.QUEUE_SERVICE_PATH)
}

func (cf *ClientFactory) NewQueueClient(url string) (queue.QueueService) {
	transFactory := NewTHttpClientTransportFactory(url, cf.credential, cf.httpClient, cf.agent)
	return &QueueClientProxy{factory: transFactory}
}

func (cf *ClientFactory) NewDefaultMessageClient() (message.MessageService) {
	return cf.NewMessageClient(constants.DEFAULT_SERVICE_ENDPOINT + constants.MESSAGE_SERVICE_PATH)
}

func (cf *ClientFactory) NewDefaultSecureMessageClient() (message.MessageService) {
	return cf.NewMessageClient(constants.DEFAULT_SECURE_SERVICE_ENDPOINT + constants.MESSAGE_SERVICE_PATH)
}

func (cf *ClientFactory) NewMessageClient(url string) (message.MessageService) {
	transFactory := NewTHttpClientTransportFactory(url, cf.credential, cf.httpClient, cf.agent)
	return &MessageClientProxy{factory: transFactory}
}


type QueueClientProxy struct {
	factory     *EmqTHttpClientTransportFactory
}

func (p *QueueClientProxy) GetServiceVersion() (r *common.Version, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=getServerVersion")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.GetServiceVersion()
}

func (p *QueueClientProxy) ValidClientVersion(clientVersion *common.Version) (err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=validateClientVersion")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.ValidClientVersion(clientVersion)
}


func (p *QueueClientProxy) CreateQueue(request *queue.CreateQueueRequest) (r *queue.CreateQueueResponse, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=createQueue")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.CreateQueue(request)
}

func (p *QueueClientProxy) DeleteQueue(request *queue.DeleteQueueRequest) (err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=deleteQueue")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.DeleteQueue(request)
}

func (p *QueueClientProxy) PurgeQueue(request *queue.PurgeQueueRequest) (err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=purgeQueue")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.PurgeQueue(request)
}

func (p *QueueClientProxy) SetQueueAttribute(request *queue.SetQueueAttributesRequest) (r *queue.SetQueueAttributesResponse, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=setQueueAttribute")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.SetQueueAttribute(request)
}

func (p *QueueClientProxy) SetQueueQuota(request *queue.SetQueueQuotaRequest) (r *queue.SetQueueQuotaResponse, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=setQueueQuota")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.SetQueueQuota(request)
}

func (p *QueueClientProxy) GetQueueInfo(request *queue.GetQueueInfoRequest) (r *queue.GetQueueInfoResponse, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=getQueueInfo")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.GetQueueInfo(request)
}

func (p *QueueClientProxy) ListQueue(request *queue.ListQueueRequest) (r *queue.ListQueueResponse, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=listQueue")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.ListQueue(request)
}

func (p *QueueClientProxy) SetQueueRedrivePolicy(request *queue.SetQueueRedrivePolicyRequest) (r *queue.SetQueueRedrivePolicyResponse, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=setQueueRedrivePolicy")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.SetQueueRedrivePolicy(request)
}

func (p *QueueClientProxy) RemoveQueueRedrivePolicy(request *queue.RemoveQueueRedrivePolicyRequest) (err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=removeQueueRedrivePolicy")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.RemoveQueueRedrivePolicy(request)
}

func (p *QueueClientProxy) SetPermission(request *queue.SetPermissionRequest) (err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=setPermission")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.SetPermission(request)
}

func (p *QueueClientProxy) RevokePermission(request *queue.RevokePermissionRequest) (err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=revokePermission")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.RevokePermission(request)
}

func (p *QueueClientProxy) QueryPermission(request *queue.QueryPermissionRequest) (r *queue.QueryPermissionResponse, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=queryPermission")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.QueryPermission(request)
}


func (p *QueueClientProxy) QueryPermissionForId(request *queue.QueryPermissionForIdRequest) (r *queue.QueryPermissionForIdResponse, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=queryPermissionForId")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.QueryPermissionForId(request)
}

func (p *QueueClientProxy) ListPermissions(request *queue.ListPermissionsRequest) (r *queue.ListPermissionsResponse, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=listPermissions")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.ListPermissions(request)
}

func (p *QueueClientProxy) CreateTag(request *queue.CreateTagRequest) (r *queue.CreateTagResponse, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=createTag")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.CreateTag(request)
}

func (p *QueueClientProxy) DeleteTag(request *queue.DeleteTagRequest) (err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=deleteTag")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.DeleteTag(request)
}

func (p *QueueClientProxy) GetTagInfo(request *queue.GetTagInfoRequest) (r *queue.GetTagInfoResponse, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=getTagInfo")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.GetTagInfo(request)
}

func (p *QueueClientProxy) ListTag(request *queue.ListTagRequest) (r *queue.ListTagResponse, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=listTag")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.ListTag(request)
}

func (p *QueueClientProxy) QueryMetric(request *queue.QueryMetricRequest) (r *queue.TimeSeriesData, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=queryMetric")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.QueryMetric(request)
}

func (p *QueueClientProxy) QueryPrivilegedQueue(request *queue.QueryPrivilegedQueueRequest) (r *queue.QueryPrivilegedQueueResponse, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=queryPrivilegedQueue")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.QueryPrivilegedQueue(request)
}

func (p *QueueClientProxy) VerifyEMQAdmin() (r *queue.VerifyEMQAdminResponse, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=verifyEMQAdmin")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.VerifyEMQAdmin()
}

func (p *QueueClientProxy) VerifyEMQAdminRole(request *queue.VerifyEMQAdminRoleRequest) (r *queue.VerifyEMQAdminRoleResponse, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=verifyEMQAdminRole")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.VerifyEMQAdminRole(request)
}

func (p *QueueClientProxy) CopyQueue(request *queue.CopyQueueRequest) (err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=copyQueue")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.CopyQueue(request)
}

func (p *QueueClientProxy) GetQueueMeta(queueName string) (r *queue.GetQueueMetaResponse, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=getQueueMeta")
	defer trans.Close()
	client := queue.NewQueueServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.GetQueueMeta(queueName)
}


type MessageClientProxy struct {
	factory     *EmqTHttpClientTransportFactory
}

func (p *MessageClientProxy) GetServiceVersion() (r *common.Version, err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=getServerVersion")
	defer trans.Close()
	client := message.NewMessageServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.GetServiceVersion()
}

func (p *MessageClientProxy) ValidClientVersion(clientVersion *common.Version) (err error) {
	trans := p.factory.GetTransportWithQuery(nil, "type=validateClientVersion")
	defer trans.Close()
	client := message.NewMessageServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	return client.ValidClientVersion(clientVersion)
}


func (p *MessageClientProxy) SendMessage(sendMessageRequest *message.SendMessageRequest) (r *message.SendMessageResponse, err error) {
	for retry := 0; retry <= common.MAX_RETRY; {
		trans := p.factory.GetTransportWithQuery(nil, "type=sendMessage")
		defer trans.Close()
		client := message.NewMessageServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		if r, e := client.SendMessage(sendMessageRequest); e != nil {
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

func (p *MessageClientProxy) SendMessageBatch(sendMessageBatchRequest *message.SendMessageBatchRequest) (r *message.SendMessageBatchResponse, err error) {
	for retry := 0; retry <= common.MAX_RETRY; {
		trans := p.factory.GetTransportWithQuery(nil, "type=sendMessageBatch")
		defer trans.Close()
		client := message.NewMessageServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		if r, e := client.SendMessageBatch(sendMessageBatchRequest); e != nil {
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

func (p *MessageClientProxy) ReceiveMessage(receiveMessageRequest *message.ReceiveMessageRequest) (r []*message.ReceiveMessageResponse, err error) {
	for retry := 0; retry <= common.MAX_RETRY; {
		trans := p.factory.GetTransportWithQuery(nil, "type=receiveMessage")
		defer trans.Close()
		client := message.NewMessageServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		if r, e := client.ReceiveMessage(receiveMessageRequest); e != nil {
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

func (p *MessageClientProxy) ChangeMessageVisibilitySeconds(changeMessageVisibilityRequest *message.ChangeMessageVisibilityRequest) (err error) {
	for retry := 0; retry <= common.MAX_RETRY; {
		trans := p.factory.GetTransportWithQuery(nil, "type=changeMessageVisibilitySeconds")
		defer trans.Close()
		client := message.NewMessageServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		if e := client.ChangeMessageVisibilitySeconds(changeMessageVisibilityRequest); e != nil {
			if p.shouldRetry(e) {
				err = e
				retry += 1
				continue
			}
			return e
		} else {
			return e
		}
	}
	return err
}

func (p *MessageClientProxy) ChangeMessageVisibilitySecondsBatch(changeMessageVisibilityBatchRequest *message.ChangeMessageVisibilityBatchRequest) (r *message.ChangeMessageVisibilityBatchResponse, err error) {
	for retry := 0; retry <= common.MAX_RETRY; {
		trans := p.factory.GetTransportWithQuery(nil, "type=changeMessageVisibilitySecondsBatch")
		defer trans.Close()
		client := message.NewMessageServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		if r, e := client.ChangeMessageVisibilitySecondsBatch(changeMessageVisibilityBatchRequest); e != nil {
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

func (p *MessageClientProxy) DeleteMessage(deleteMessageRequest *message.DeleteMessageRequest) (err error) {
	for retry := 0; retry <= common.MAX_RETRY; {
		trans := p.factory.GetTransportWithQuery(nil, "type=deleteMessage")
		defer trans.Close()
		client := message.NewMessageServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		if e := client.DeleteMessage(deleteMessageRequest); e != nil {
			if p.shouldRetry(e) {
				err = e
				retry += 1
				continue
			}
			return e
		} else {
			return e
		}
	}
	return err
}

func (p *MessageClientProxy) DeleteMessageBatch(deleteMessageBatchRequest *message.DeleteMessageBatchRequest) (r *message.DeleteMessageBatchResponse, err error) {
	for retry := 0; retry <= common.MAX_RETRY; {
		trans := p.factory.GetTransportWithQuery(nil, "type=deleteMessageBatch")
		defer trans.Close()
		client := message.NewMessageServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		if r, e := client.DeleteMessageBatch(deleteMessageBatchRequest); e != nil {
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


func (p *MessageClientProxy) DeadMessage(deadMessageRequest *message.DeadMessageRequest) (err error) {
	for retry := 0; retry <= common.MAX_RETRY; {
		trans := p.factory.GetTransportWithQuery(nil, "type=deadMessage")
		defer trans.Close()
		client := message.NewMessageServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		if e := client.DeadMessage(deadMessageRequest); e != nil {
			if p.shouldRetry(e) {
				err = e
				retry += 1
				continue
			}
			return e
		} else {
			return e
		}
	}
	return err
}

func (p *MessageClientProxy) DeadMessageBatch(deadMessageBatchRequest *message.DeadMessageBatchRequest) (r *message.DeadMessageBatchResponse, err error) {
	for retry := 0; retry <= common.MAX_RETRY; {
		trans := p.factory.GetTransportWithQuery(nil, "type=deadMessageBatch")
		defer trans.Close()
		client := message.NewMessageServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		if r, e := client.DeadMessageBatch(deadMessageBatchRequest); e != nil {
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


func (p *MessageClientProxy) PeekMessage(peekMessageRequest *message.PeekMessageRequest) (r []*message.PeekMessageResponse, err error) {
	for retry := 0; retry <= common.MAX_RETRY; {
		trans := p.factory.GetTransportWithQuery(nil, "type=peekMessage")
		defer trans.Close()
		client := message.NewMessageServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		if r, e := client.PeekMessage(peekMessageRequest); e != nil {
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

func (p *MessageClientProxy) DeletePeekMessage(deletePeekMessageRequest *message.DeletePeekMessageRequest) (err error) {
	for retry := 0; retry <= common.MAX_RETRY; {
		trans := p.factory.GetTransportWithQuery(nil, "type=deletePeekMessage")
		defer trans.Close()
		client := message.NewMessageServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		if e := client.DeletePeekMessage(deletePeekMessageRequest); e != nil {
			if p.shouldRetry(e) {
				err = e
				retry += 1
				continue
			}
			return e
		} else {
			return e
		}
	}
	return err
}

func (p *MessageClientProxy) DeletePeekMessageBatch(deletePeekMessageBatchRequest *message.DeletePeekMessageBatchRequest) (r *message.DeletePeekMessageBatchResponse, err error) {
	for retry := 0; retry <= common.MAX_RETRY; {
		trans := p.factory.GetTransportWithQuery(nil, "type=deletePeekMessageBatch")
		defer trans.Close()
		client := message.NewMessageServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		if r, e := client.DeletePeekMessageBatch(deletePeekMessageBatchRequest); e != nil {
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

func (p *MessageClientProxy) shouldRetry(err error) bool {
	if se, ok := err.(EmqErrorCodePeeker); ok {
		if backoff, ok := common.ERROR_BACKOFF[se.GetErrorCode()]; ok && backoff > 0 {
			duration := time.Duration(int64(backoff) * int64(time.Millisecond))
			glog.Infof("Backoff with %s and retry due to error %s", duration, se.GetErrorCode())
			time.Sleep(duration)
			return true
		}
	}
	return false
}

