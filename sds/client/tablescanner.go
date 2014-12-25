package client

import (
	"time"
	"github.com/XiaoMi/galaxy-sdk-go/sds/errors"
	"github.com/XiaoMi/galaxy-sdk-go/sds/table"
	"github.com/golang/glog"
)

type TableScanner struct {
	client table.TableService
	scan   *table.ScanRequest
}

func NewTableScanner(client table.TableService, scan *table.ScanRequest) (*TableScanner) {
	return &TableScanner{client: client, scan: scan}
}

type ScannedItem struct {
	Datum map[string]*table.Datum
	Error error
}

// This will end up with data copies and synchronizations, however we are IO bounded
func (p *TableScanner) Iter() <-chan *ScannedItem {
	sz := p.scan.GetLimit()
	if sz < 1 {
		p.scan.Limit = 1
		sz = 1
	}
	if sz > 1000 {
		sz = 1000
	}
	ch := make(chan *ScannedItem, sz)

	go func() {
		retry := 0
		for {
			if retry > 0 {
				duration := time.Duration(int64(1 << (uint(retry - 1))) * int64(time.Millisecond) *
						int64(errors.ERROR_BACKOFF[errors.ErrorCode_THROUGHPUT_EXCEED]))
				glog.Infof("Throttled and slepping for: %s", duration)
				time.Sleep(duration)
			}
			if res, err := p.client.Scan(p.scan); err != nil {
				ch <- &ScannedItem{Datum: nil, Error: err}
				break;
			} else {
				done := res.GetNextStartKey() == nil
				if !done && len(res.GetRecords()) < int(p.scan.GetLimit()) {
					// request been throttled
					retry += 1
				} else {
					retry = 0
				}
				for _, item := range res.GetRecords() {
					ch <- &ScannedItem{Datum: item, Error: nil}
				}
				if done {
					break;
				}
				p.scan.StartKey = res.GetNextStartKey()
			}
		}
		close(ch)
	}()
	time.Sleep(time.Second)
	return ch
}
