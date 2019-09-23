package fields

import (
	"net"
	"strconv"
	"strings"
	"time"
)

type StringNet struct{ net.IPNet }

func (t *StringNet) UnmarshalJSON(b []byte) error {
	raw, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	_, net, err := net.ParseCIDR(strings.Replace(raw, `\`, "", 2))

	if err != nil {
		return err
	}
	t.IPNet = *net
	return nil
}

type StringIP struct{ net.IP }

func (t *StringIP) UnmarshalJSON(b []byte) error {
	ip, err := ParseStringIP(string(b))
	if err != nil {
		return err
	}
	t.IP = ip
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

// Helpers

func ParseStringIP(textual string) (net.IP, error) {
	raw, err := strconv.Unquote(textual)
	if err != nil {
		return nil, err
	}
	return net.ParseIP(raw), nil
}
