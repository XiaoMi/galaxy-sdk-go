package client

import (
	"fmt"
	"github.com/XiaoMi/galaxy-sdk-go/sds/errors"
)

type SdsErrorCodePeeker interface {
	GetErrorCode() errors.ErrorCode
}

type SdsTransportError struct {
	HttpStatusCode errors.HttpStatusCode
	ErrorCode      errors.ErrorCode
	ErrorMessage   string
	ServerTime     int64
}

func NewSdsTransportError(httpStatusCode errors.HttpStatusCode,
	errorMessage string, timestamp int64) *SdsTransportError {
	errorCode := errors.ErrorCode_UNKNOWN
	if httpStatusCode == errors.HttpStatusCode_INVALID_AUTH {
		errorCode = errors.ErrorCode_INVALID_AUTH
	} else if httpStatusCode == errors.HttpStatusCode_CLOCK_TOO_SKEWED {
		errorCode = errors.ErrorCode_CLOCK_TOO_SKEWED
	} else if httpStatusCode == errors.HttpStatusCode_REQUEST_TOO_LARGE {
		errorCode = errors.ErrorCode_REQUEST_TOO_LARGE
	} else if httpStatusCode == errors.HttpStatusCode_INTERNAL_ERROR {
		errorCode = errors.ErrorCode_INTERNAL_ERROR
	} else if httpStatusCode == errors.HttpStatusCode_BAD_REQUEST {
		errorCode = errors.ErrorCode_BAD_REQUEST
	}
	return &SdsTransportError{
		HttpStatusCode: httpStatusCode,
		ErrorCode: errorCode,
		ErrorMessage: errorMessage,
		ServerTime: timestamp,
	}
}

func (p *SdsTransportError) GetErrorCode() errors.ErrorCode {
	return p.ErrorCode
}

func (p *SdsTransportError) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Transport error, ErrorCode: %s, HttpStatusCode: %s, ErrorMessage: %s",
		p.ErrorCode, p.HttpStatusCode, p.ErrorMessage)
}

func (p *SdsTransportError) Error() string {
	return p.String()
}
