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
	if err != nil {
		t.Time, err = time.Parse("2006-01-02T15:04:05.999999Z0700", raw)
	}
	return err
}

// MarshalJSON ensures that timestamps are re-encoded the way Suricata made them
func (t *QuotedRFC3339) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.Time.Format("2006-01-02T15:04:05.000000-0700") + `"`), nil
}

// Helpers

func ParseStringIP(textual string) (net.IP, error) {
	raw, err := strconv.Unquote(textual)
	if err != nil {
		return nil, err
	}
	return net.ParseIP(raw), nil
}
