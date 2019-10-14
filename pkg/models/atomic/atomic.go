package atomic

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ccdcoe/go-peek/pkg/models/fields"
)

type Event interface {
	// Time implements atomic.Event
	// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
	Time() time.Time
	// Source implements atomic.Event
	// Source of message, usually emitting program
	Source() string
	// Sender implements atomic.Event
	// Sender of message, usually a host
	Sender() string
}

// Messager is for accessing raw unstructure or human-formatted payload of event
// Useful for simple logging for humans or for text mining methods
type Messager interface {
	// Message implements atomic.Messager
	// For example, syslog msg field or Suricata EVE fields in fast log format
	Message() string
}

// JSONFormatter is for re-encoding parsed messages to byte array without knowng the payload type
type JSONFormatter interface {
	// JSONFormat implements atomic.JSONFormatter by wrapping json.Marshal
	JSONFormat() ([]byte, error)
}

// CSVFormatter is for encoding parsed messages in CSV format, for easy database dumps and excel stuff
// Output should integrate easily with encoding/csv Write() method
type CSVFormatter interface {
	CSVHeader() []string
	CSVFormat() []string
}

type ErrSyslogMsgParse struct {
	Err    error
	Desc   string
	Offset int
	Buf    string
}

func (e ErrSyslogMsgParse) Error() string {
	return fmt.Sprintf("Cannot parse syslog msg [%s], offset %d. Err: [%s]", e.Buf, e.Offset, e.Err)
}

// ParseSyslog is used to extract known events that are usually packaged in syslog payload
// unsupported messages will be passed through cleanly
// expects a type switch on receiving end
func ParseSyslogMessage(s Syslog) (interface{}, error) {
	if strings.HasPrefix(s.Message, "@cee: ") || strings.HasPrefix(s.Message, " @cee: ") {
		switch s.Program {
		case "suricata":
			var obj StaticSuricataEve
			if err := json.Unmarshal(getCommonEventExpressionPayload(s.Message), &obj); err != nil {
				return nil, err
			}
			return obj, nil
		default:
			// Assume Windows Event Log
			e, err := NewWindowsEventLog(getCommonEventExpressionPayload(s.Message))
			if err != nil {
				return nil, err
			}
			return e, nil
		}
	}
	switch s.Program {
	case "snoopy":
		obj, err := ParseSnoopy(s.Message)
		if err != nil {
			return obj, err
		}
		return obj, nil
	}
	if s.Program == "CEF" {
		switch s.Host {
		case "MazeRunner":

		case "Mendel":

		}
	}
	return s, nil
}

func getCommonEventExpressionPayload(raw string) []byte {
	return []byte(strings.TrimPrefix(strings.TrimLeft(raw, " "), "@cee: "))
}

type ErrParseSnoopy struct {
	Msg    string
	Reason string
}

func (e ErrParseSnoopy) Error() string {
	return fmt.Sprintf("Unable to parse snoopy msg [%s] BECAUSE: %s", e.Msg, e.Reason)
}

func ParseSnoopy(m string) (*Snoopy, error) {
	if strings.HasPrefix(m, " ") {
		m = strings.TrimLeft(m, " ")
	}
	if !strings.HasPrefix(m, "[") {
		return nil, ErrParseSnoopy{Msg: m, Reason: "Invalid header"}
	}
	c := strings.Index(m, "]")
	if c == -1 {
		return nil, ErrParseSnoopy{Msg: m, Reason: "No closing bracket"}
	}
	sub := false
	f := func(c rune) bool {
		switch {
		case c == '(':
			sub = !sub
			return false
		case c == ')':
			sub = !sub
			return true
		case sub:
			return false
		default:
			return c == ' '
		}
	}
	data := strings.TrimLeft(m[:c], "[")
	cmd := m[c:]
	obj := &Snoopy{}
	if i := strings.Index(cmd, ":"); i != -1 && len(cmd) > 3 {
		obj.Cmd = cmd[i+1:]
	}

	//fields := strings.Fields(data)
	items := strings.FieldsFunc(data, f)
	switch len(items) {
	case 10:
		// custom config with extended info
		bites := make([]string, 10)
		for i, f := range items {
			if val := strings.SplitN(f, ":", 2); len(val) != 2 {
				continue
			} else if i == 1 && strings.HasPrefix(f, "ssh") {
				if cuts := strings.Split(f, "("); len(cuts) == 2 {
					if cuts = strings.Fields(cuts[1]); len(cuts) == 4 {
						obj.SSH = &SnoopySSH{
							SrcIP:   &fields.StringIP{IP: net.ParseIP(cuts[0])},
							SrcPort: cuts[1],
							DstIP:   &fields.StringIP{IP: net.ParseIP(cuts[2])},
							DstPort: cuts[3],
						}
					}
				}
			} else {
				bites[i] = val[1]
			}
			obj.Login = bites[0]
			obj.Username = bites[2]
			obj.UID = bites[3]
			obj.Group = bites[4]
			obj.Gid = bites[5]
			obj.Sid = bites[6]
			obj.Tty = bites[7]
			obj.Cwd = bites[8]
			obj.Filename = bites[9]
		}
	case 5:
		// default config
		bites := make([]string, 5)
		for i, f := range items {
			if val := strings.SplitN(f, ":", 2); len(val) == 2 {
				bites[i] = val[1]
			}
		}
		obj.UID = bites[0]
		obj.Sid = bites[1]
		obj.Tty = strings.TrimLeft(bites[2], "(")
		obj.Cwd = bites[3]
		obj.Filename = bites[4]
	default:
		return nil, ErrParseSnoopy{Msg: m}
	}
	return obj, nil
}
