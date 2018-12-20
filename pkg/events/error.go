package events

import "fmt"

type EventParseErr struct {
	key string
	err error
	raw []byte
}

func (e EventParseErr) Error() string {
	return fmt.Sprintf("ERROR accessing key: %s ERRVAL: [%s] RAW: [%s]", e.key, e.Error(), string(e.raw))
}
