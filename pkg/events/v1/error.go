package events

import "fmt"

type ErrDecode struct {
	raw []byte
	err error
}

func (e ErrDecode) Error() string {
	return fmt.Sprintf("Failed to decode [%s], [%s]", string(e.raw), e.err.Error())
}

type EventParseErr struct {
	key string
	err error
	raw []byte
}

func (e EventParseErr) Error() string {
	return fmt.Sprintf("ERROR accessing key: %s ERRVAL: [%s] RAW: [%s]", e.key, e.Error(), string(e.raw))
}

type ErrParseIP struct {
	ip    string
	event EventFormatter
}

func (e ErrParseIP) Error() string {
	raw, _ := e.event.JSON()
	return fmt.Sprintf("failed to parse string %s as net.IP. Event is [%s]", string(raw))
}
