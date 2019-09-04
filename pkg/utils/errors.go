package utils

import (
	"fmt"
)

// TODO: close channel after drain
// should be done here, not by receiver
// range loop will deadlock
type ErrChan struct {
	Desc  string
	Items chan error
}

func (e ErrChan) Error() string {
	return fmt.Sprintf(
		"%s experienced %d errors, please drain items channel for more information",
		e.Desc,
		len(e.Items),
	)
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
