package atomic

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ccdcoe/go-peek/pkg/models/fields"
)

type Syslog struct {
	Timestamp time.Time        `json:"@timestamp"`
	Host      string           `json:"syslog_host"`
	Program   string           `json:"syslog_program"`
	Severity  string           `json:"syslog_severity"`
	Facility  string           `json:"syslog_facility"`
	Message   string           `json:"syslog_message"`
	IP        *fields.StringIP `json:"syslog_ip,omitempty"`
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (s Syslog) Time() time.Time { return s.Timestamp }

// Source implements atomic.Event
// Source of message, usually emitting program
func (s Syslog) Source() string { return s.Program }

// Sender implements atomic.Event
// Sender of message, usually a host
func (s Syslog) Sender() string { return s.Host }

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
	if isCommonEventExpression(s.Message) {
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
	case "CEF":
		// Some vendors have a thing where hostname duplicates as program and CEF: is stuck as program name in syslog header
		switch s.Host {
		case "MazeRunner", "Mendel":
			obj, err := ParseCEF(s.Message)
			if err != nil {
				return obj, err
			}
			return obj, nil
		}
	}
	return s, nil
}
