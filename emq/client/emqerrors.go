package client


import (
	"fmt"
	"github.com/XiaoMi/galaxy-sdk-go/emq/common"
	"github.com/XiaoMi/galaxy-sdk-go/rpc/errors"
)

type EmqErrorCodePeeker interface {
	GetErrorCode() common.ErrorCode
}

type EmqTransportError struct {
	HttpStatusCode errors.HttpStatusCode
	ErrorCode      common.ErrorCode
	ErrorMessage   string
	ServerTime     int64
}

func NewEmqTransportError(httpStatusCode errors.HttpStatusCode,
	errorMessage string, timestamp int64) *EmqTransportError {
	errorCode := common.ErrorCode_UNKNOWN
	if httpStatusCode == errors.HttpStatusCode_INVALID_AUTH {
		errorCode = common.ErrorCode_UNKNOWN
	} else if httpStatusCode == errors.HttpStatusCode_CLOCK_TOO_SKEWED {
		errorCode = common.ErrorCode_UNKNOWN
	} else if httpStatusCode == errors.HttpStatusCode_REQUEST_TOO_LARGE {
		errorCode = common.ErrorCode_BAD_REQUEST
	} else if httpStatusCode == errors.HttpStatusCode_INTERNAL_ERROR {
		errorCode = common.ErrorCode_INTERNAL_ERROR
	} else if httpStatusCode == errors.HttpStatusCode_BAD_REQUEST {
		errorCode = common.ErrorCode_BAD_REQUEST
	}
	return &EmqTransportError{
		HttpStatusCode: httpStatusCode,
		ErrorCode: errorCode,
		ErrorMessage: errorMessage,
		ServerTime: timestamp,
	}
}

func (p *EmqTransportError) GetErrorCode() common.ErrorCode {
	return p.ErrorCode
}

func (p *EmqTransportError) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Transport error, ErrorCode: %s, HttpStatusCode: %s, ErrorMessage: %s",
		p.ErrorCode, p.HttpStatusCode, p.ErrorMessage)
}

func (p *EmqTransportError) Error() string {
	return p.String()
}