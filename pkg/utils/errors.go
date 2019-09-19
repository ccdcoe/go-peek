package utils

import (
	"fmt"
	"sync"
)

// TODO: close channel after drain
// should be done here, not by receiver
// range loop will deadlock
type ErrChan struct {
	*sync.Mutex
	Desc  string
	Items chan error
	Total int
	Max   int
}

func (e ErrChan) Error() string {
	return fmt.Sprintf(
		"%s experienced %d errors total, %d available for reporting limited to configured max %d, please drain items channel for more information",
		e.Desc,
		e.Total,
		len(e.Items),
		e.Max,
	)
}

func (e *ErrChan) Send(err error) *ErrChan {
	if e.Max < 10 {
		e.Max = 10
	}
	if e.Mutex == nil {
		e.Mutex = &sync.Mutex{}
	}
	e.Lock()
	defer e.Unlock()
	if e.Items == nil {
		e.Items = make(chan error, e.Max)
	}
	if len(e.Items) >= e.Max {
		<-e.Items
	}
	e.Items <- err
	e.Total++
	return e
}

type ErrNilPointer struct {
	Function, Caller string
}

func (e ErrNilPointer) Error() string {
	return fmt.Sprintf(
		"Nil pointer in %s while calling %s",
		e.Caller, e.Function,
	)
}

// ErrParseMessageSource is a custom error type for cases where reporting individual errors is not practical
// For example, when reading log files with millions of entrys where some lines may be broken due to force majuere
// User needs to be informed about these errors without spam
// Optionally, indivudual errors can be attached as ErrChan object
// Sampling errors may be a good idea if it is used, though
type ErrParseMessageSource struct {
	Count  int64
	Source string
	Parser string
	Errs   *ErrChan
}

func (e ErrParseMessageSource) Error() string {
	msg := fmt.Sprintf(
		"[%d] errors were encountered while parsing source [%s] with parser [%s]",
		e.Count,
		e.Source,
		e.Parser,
	)
	if e.Errs != nil && len(e.Errs.Items) > 0 {
		msg = fmt.Sprintf(
			"%s; dndividual errors attached as channel with %d elements; drain Errs.Items to see messages",
			msg,
			len(e.Errs.Items),
		)
	}
	return msg
}

// ErrParseRawData is a custom error type when something went wrong wile parsing a raw []byte into an event struct
// Line number/message counter, source file/topic/channel, etc should be attached for debug
// Desc should describe the calling function and purpose
type ErrParseRawData struct {
	Err    error
	Raw    []byte
	Source string
	Offset int64
	Desc   string
}

func (e ErrParseRawData) Error() string {
	return fmt.Sprintf(
		"Error: [%s] parsing message [%s] from [%s] offset [%d]; desc: [%s]",
		e.Err,
		string(e.Raw),
		e.Source,
		e.Offset,
		e.Desc,
	)
}

type ErrInvalidPath struct {
	Path, Msg string
}

func (e ErrInvalidPath) Error() string {
	return fmt.Sprintf("path error for %s: %s", e.Path, e.Msg)
}

type ErrDecodeJson struct {
	Err error
	Raw []byte
}

func (e ErrDecodeJson) Error() string {
	return fmt.Sprintf("%s for [%s]", e.Err, string(e.Raw))
}
