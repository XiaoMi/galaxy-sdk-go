package main

import (
	"flag"
	"fmt"
	"time"
	"github.com/XiaoMi/galaxy-sdk-go/rpc/auth"
	"github.com/XiaoMi/galaxy-sdk-go/emq/client"
	"github.com/XiaoMi/galaxy-sdk-go/emq/queue"
	"github.com/XiaoMi/galaxy-sdk-go/emq/message"
	"github.com/XiaoMi/galaxy-sdk-go/emq/constants"
	"github.com/XiaoMi/galaxy-sdk-go/thrift"
)


func main() {
	flag.Parse()

	// Set your AppKey and AppSecret
	appKey := ""
	appSecret := ""
	userType := auth.UserType_APP_SECRET
	cred := auth.Credential{&userType, &appKey, thrift.StringPtr(appSecret)}
	endpoint := ""

	clientFactory := client.NewClientFactory(&cred, time.Duration(constants.DEFAULT_CLIENT_SOCKET_TIMEOUT * int64(time.Second)))

	queueClient := clientFactory.NewQueueClient(endpoint + constants.QUEUE_SERVICE_PATH)

	messageClient := clientFactory.NewMessageClient(endpoint + constants.MESSAGE_SERVICE_PATH)

	queueName := "testGoExampleQueue"
	var readCapacity int64 = 10
	var writeCapacity int64 = 10

	createQueueRequest := queue.CreateQueueRequest {
		QueueName : queueName,
		QueueAttribute : queue.NewQueueAttribute(),
		QueueQuota : &queue.QueueQuota {
			Throughput : &queue.Throughput {
				ReadQps : &readCapacity,
				WriteQps : &writeCapacity,
			},
		},
	}

	if createQueueResponse, err := queueClient.CreateQueue(&createQueueRequest); err != nil {
		fmt.Printf("Failed to CreateQueue: %s\n", err)
	} else {
		queueName = createQueueResponse.GetQueueName()
		fmt.Printf("create queue: %s\n", queueName)
		listQueueRequest := queue.ListQueueRequest{
			QueueNamePrefix : "test",
		}
		if listQueueResponse, err := queueClient.ListQueue(&listQueueRequest); err != nil {
			fmt.Printf("Failed to ListQueue: %s\n", err)
		} else {
			for i := 0; i < len(listQueueResponse.GetQueueName()); i += 1 {
				fmt.Printf("has queue: %s\n", listQueueResponse.GetQueueName()[i])
			}
		}
	}

	tagName := "tagTest"
	createTagRequest := queue.CreateTagRequest{
		QueueName : queueName,
		TagName : tagName,
	}
	_, err := queueClient.CreateTag(&createTagRequest)
	if err != nil {
		fmt.Printf("Failed to CreateTag: %s\n", err)
	} else {
		fmt.Printf("created tag %s\n", tagName)
	}

	fmt.Printf("== SEND OPERATION ==\n")
	messageBody := "test message body"
	var delaySeconds int32 = 2
	sendMessageRequest := message.SendMessageRequest{
		QueueName : queueName,
		MessageBody : messageBody,
		DelaySeconds : &delaySeconds,
	}
	sendMessageResponse, err := messageClient.SendMessage(&sendMessageRequest)
	if err != nil {
		fmt.Printf("Failed to SendMessage: %s\n", err)
	} else {
		fmt.Printf("Message Id:%s\n", sendMessageResponse.GetMessageID())
		fmt.Printf("Message Body MD5:%s\n", sendMessageResponse.GetBodyMd5())
		fmt.Printf("Message Body:%s\n", messageBody)
	}
	time.Sleep(time.Second * 3)

	fmt.Printf("== RECEIVE FROM QUEUE DEFAULT ==\n")
	receiveMessageRequest := message.ReceiveMessageRequest{
		QueueName : queueName,
		MaxReceiveMessageNumber : 3,
	}
	var receiveMessageResponse []*message.ReceiveMessageResponse
	for true {
		receiveMessageResponse, err = messageClient.ReceiveMessage(&receiveMessageRequest)
		if err != nil {
			fmt.Printf("Failed to ReceiveMessage: %s\n", err)
			break
		} else {
			if receiveMessageResponse != nil {
				break
			}
		}
	}
	var entryList []*message.DeleteMessageBatchRequestEntry
	for i := 0; i < len(receiveMessageResponse); i += 1 {
		fmt.Printf("Message Id:%s\n", receiveMessageResponse[i].GetMessageID())
		fmt.Printf("Message ReceiptHandle:%s\n", receiveMessageResponse[i].GetReceiptHandle())
		fmt.Printf("Message Body:%s\n", receiveMessageResponse[i].GetMessageBody())
		entryList = append(entryList, &message.DeleteMessageBatchRequestEntry{
			ReceiptHandle : receiveMessageResponse[i].GetReceiptHandle(),
		})
	}
	deleteMessageBatchRequest := message.DeleteMessageBatchRequest {
		QueueName : queueName,
		DeleteMessageBatchRequestEntryList : entryList,
	}
	_, err = messageClient.DeleteMessageBatch(&deleteMessageBatchRequest)
	if err != nil {
		fmt.Printf("Failed to DeleteMessageBatch: %s\n", err)
	}


	fmt.Printf("== RECEIVE FROM QUEUE TAG ==\n")
	receiveMessageRequest = message.ReceiveMessageRequest{
		QueueName : queueName,
		TagName : &tagName,
		MaxReceiveMessageNumber : 3,
	}

	for true {
		receiveMessageResponse, err = messageClient.ReceiveMessage(&receiveMessageRequest)
		if err != nil {
			fmt.Printf("Failed to ReceiveMessage: %s\n", err)
			break
		} else {
			if receiveMessageResponse != nil {
				break
			}
		}
	}
	var entryList1 []*message.ChangeMessageVisibilityBatchRequestEntry
	for i := 0; i < len(receiveMessageResponse); i += 1 {
		fmt.Printf("Message Id:%s\n", receiveMessageResponse[i].GetMessageID())
		fmt.Printf("Message ReceiptHandle:%s\n", receiveMessageResponse[i].GetReceiptHandle())
		fmt.Printf("Message Body:%s\n", receiveMessageResponse[i].GetMessageBody())
		entryList1 = append(entryList1, &message.ChangeMessageVisibilityBatchRequestEntry{
			ReceiptHandle : receiveMessageResponse[i].GetReceiptHandle(),
			InvisibilitySeconds : 0,
		})
	}

	changeMessageVisibilityBatchRequest := message.ChangeMessageVisibilityBatchRequest{
		QueueName : queueName,
		ChangeMessageVisibilityRequestEntryList : entryList1,
	}

	_, err = messageClient.ChangeMessageVisibilitySecondsBatch(&changeMessageVisibilityBatchRequest)
	if err != nil {
		fmt.Printf("Failed to ChangeMessageVisibilityBatch: %s\n", err)
	}


	fmt.Printf("== DELETE OPERATION ==\n")
	deleteQueueRequest := queue.DeleteQueueRequest {
		QueueName : queueName,
	}
	queueClient.DeleteQueue(&deleteQueueRequest)
}