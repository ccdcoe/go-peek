package fields

import (
	"net"
	"strconv"
	"time"
)

// Custom extracted fields
type StringIP struct{ net.IP }

func (t *StringIP) UnmarshalJSON(b []byte) error {
	raw, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	t.IP = net.ParseIP(raw)
	return nil
}

type QuotedRFC3339 struct{ time.Time }

func (t *QuotedRFC3339) UnmarshalJSON(b []byte) error {
	raw, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	t.Time, err = time.Parse("2006-01-02T15:04:05.999999-0700", raw)
	return err
}
