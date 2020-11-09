package parsers

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go-peek/pkg/models/atomic"
	"go-peek/pkg/models/consumer"
	"go-peek/pkg/models/events"

	"github.com/influxdata/go-syslog/v3/rfc5424"
)

func Parse(data []byte, enum events.Atomic, p consumer.Parser) (interface{}, error) {
	if p == consumer.RFC5424 {
		return ParseSyslogGameEvent(data, enum)
	}
	if p == consumer.RawJSON {
		return UnmarshalStructuredEvent(data, enum)
	}
	return nil, fmt.Errorf("UNSUPPORTED %s, %s for [%s]", p, enum, string(data))
}

func ParseSyslogGameEvent(data []byte, enum events.Atomic) (interface{}, error) {
	machine := rfc5424.NewParser(rfc5424.WithBestEffort())
	msg, err := machine.Parse(data)
	if err != nil {
		return nil, err
	}
	strictMsg, ok := msg.(*rfc5424.SyslogMessage)
	if !ok {
		// Should never hit this, but I don't like leaving it without a check
		return nil, errors.New("Syslog message did not parse as rfc5424")
	}

	s := atomic.Syslog{
		Timestamp: func() time.Time {
			if tx := strictMsg.Timestamp; tx != nil {
				return *tx
			}
			return time.Now()
		}(),
		Message: func() string {
			if tx := strictMsg.Message; tx != nil {
				return *tx
			}
			return ""
		}(),
		Host: func() string {
			if tx := strictMsg.Hostname; tx != nil {
				return *tx
			}
			return ""
		}(),
		Facility: func() string {
			if tx := strictMsg.FacilityLevel(); tx != nil {
				return *tx
			}
			return ""
		}(),
		Severity: func() string {
			if tx := strictMsg.SeverityLevel(); tx != nil {
				return *tx
			}
			return ""
		}(),
		Program: func() string {
			if tx := strictMsg.Message; tx != nil {
				return *tx
			}
			return ""
		}(),
	}
	switch enum {
	case events.EventLogE, events.SysmonE, events.SuricataE:
		return UnmarshalStructuredEvent([]byte(*strictMsg.Message), enum)
	}

	payload, err := atomic.ParseSyslogMessage(s)
	if err != nil {
		return nil, err
	}
	switch val := payload.(type) {
	case *atomic.Snoopy:
		return &events.Snoopy{
			Syslog:    s,
			Snoopy:    *val,
			Timestamp: s.Time(),
		}, nil
	case atomic.Snoopy:
		return &events.Snoopy{
			Syslog:    s,
			Snoopy:    val,
			Timestamp: s.Time(),
		}, nil
	case atomic.Syslog:
		return &events.Syslog{
			Syslog: s,
		}, nil
	case *atomic.Syslog:
		return &events.Syslog{
			Syslog: s,
		}, nil
	}
	return nil, fmt.Errorf("Unsupported event type")
}

func UnmarshalStructuredEvent(data []byte, enum events.Atomic) (interface{}, error) {
	switch enum {
	case events.SuricataE:
		var obj atomic.StaticSuricataEve
		if err := json.Unmarshal([]byte(data), &obj); err != nil {
			return nil, err
		}
		return &events.Suricata{
			StaticSuricataEve: obj,
		}, nil
	case events.EventLogE, events.SysmonE:
		var obj atomic.DynamicWinlogbeat
		if err := json.Unmarshal([]byte(data), &obj); err != nil {
			return nil, err
		}
		return &events.DynamicWinlogbeat{
			Timestamp:         obj.Time(),
			DynamicWinlogbeat: obj,
		}, nil
	}
	return nil, fmt.Errorf("Unsupported structured event type")
}
