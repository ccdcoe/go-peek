package atomic

import (
	"fmt"
	"strings"
	"time"
)

type Cef struct {
	DeviceVendor  string            `json:"DeviceVendor"`
	DeviceProduct string            `json:"DeviceProduct"`
	DeviceVersion string            `json:"DeviceVersion"`
	SignatureID   string            `json:"SignatureID"`
	Name          string            `json:"Name"`
	Severity      string            `json:"Severity"`
	Extensions    map[string]string `json:"Extensions"`
}

func (c Cef) Content() map[string]string {
	if c.Extensions == nil {
		return nil
	}
	return c.Extensions
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (c Cef) Time() time.Time { return time.Time{} }

// Source implements atomic.Event
// Source of message, usually emitting program
func (c Cef) Source() string { return c.DeviceProduct }

// Sender implements atomic.Event
// Sender of message, usually a host
func (c Cef) Sender() string { return c.DeviceVendor }

type ErrParseCEF struct {
	Msg    string
	Reason string
}

func (e ErrParseCEF) Error() string {
	return fmt.Sprintf("Unable to parse CEF msg [%s] BECAUSE: %s", e.Msg, e.Reason)
}

func ParseCEF(m string) (*Cef, error) {
	if strings.HasPrefix(m, "CEF: ") {
		m = strings.TrimLeft(m, "CEF: ")
	}
	if strings.HasPrefix(m, " ") {
		m = strings.TrimLeft(m, " ")
	}
	core := strings.SplitN(m, "|", 8)
	if c := len(core); c != 8 {
		return nil, ErrParseCEF{
			Msg:    m,
			Reason: fmt.Sprintf("Field count mismatch, should be 8. Got %d", c),
		}
	}
	obj := &Cef{
		DeviceVendor:  core[1],
		DeviceProduct: core[2],
		DeviceVersion: core[3],
		SignatureID:   core[4],
		Name:          core[5],
		Severity:      core[6],
		Extensions:    make(map[string]string),
	}
	obj.Extensions = parseCommonEventFormatExtensions(core[7])
	if c := len(obj.Extensions); c == 0 {
		return obj, ErrParseCEF{
			Msg:    m,
			Reason: "Empty extension list, not legit for our use-case",
		}
	}
	return obj, nil
}

func parseCommonEventFormatExtensions(str string) map[string]string {
	var escaped bool
	var key, val, sub string
	var last, offset, offset2, found int
	data := make(map[string]string)
	for i, r := range str {
		switch r {
		case '\\':
			if !escaped {
				escaped = true
			}
		case ' ':
			offset = i
		case '=':
			if !escaped {
				sub = str[last:i]
				for j, r2 := range sub {
					if r2 == ' ' {
						offset2 = j
					}
				}
				if found > 0 && len(sub) > 1 {
					val = sub[1:offset2]
					data[strings.TrimLeft(key, " ")] = val
				}
				key = str[offset:i]
				last = i
				found++
			}
		default:
			escaped = false
		}
	}
	key = sub[offset2:]
	val = str[last:]
	if len(key) > 1 && len(val) > 1 {
		data[strings.TrimLeft(key, " ")] = val[1:]
	}
	return data
}
