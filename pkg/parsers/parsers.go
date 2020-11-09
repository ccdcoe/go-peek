package parsers

import (
	"encoding/json"
	"fmt"

	"go-peek/pkg/models/atomic"
	"go-peek/pkg/models/consumer"
	"go-peek/pkg/models/events"
	"github.com/influxdata/go-syslog/rfc5424"
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
	bestEffort := true
	msg, err := rfc5424.NewParser().Parse(data, &bestEffort)
	if err != nil {
		return nil, err
	}
	s := atomic.Syslog{
		Timestamp: *msg.Timestamp(),
		Message:   *msg.Message(),
		Host:      *msg.Hostname(),
		Program:   *msg.Appname(),
	}
	switch enum {
	case events.EventLogE, events.SysmonE, events.SuricataE:
		return UnmarshalStructuredEvent([]byte(*msg.Message()), enum)
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
