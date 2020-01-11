package parsers

import (
	"encoding/json"
	"fmt"

	"github.com/ccdcoe/go-peek/pkg/models/atomic"
	"github.com/ccdcoe/go-peek/pkg/models/events"
	"github.com/influxdata/go-syslog/rfc5424"
)

type Format int

const (
	ParseJSON Format = iota
	ParseRFC5424
)

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
	case events.SuricataE:
		var obj atomic.StaticSuricataEve
		if err := json.Unmarshal([]byte(s.Message), &obj); err != nil {
			return nil, err
		}
		return &events.Suricata{
			StaticSuricataEve: obj,
		}, nil
	case events.EventLogE, events.SysmonE:
		var obj atomic.DynamicWinlogbeat
		if err := json.Unmarshal([]byte(s.Message), &obj); err != nil {
			return nil, err
		}
		return &events.DynamicWinlogbeat{
			Timestamp:         obj.Time(),
			DynamicWinlogbeat: obj,
		}, nil
	}

	payload, err := atomic.ParseSyslogMessage(s)
	if err != nil {
		return nil, err
	}
	switch val := payload.(type) {
	case *atomic.Snoopy:
		return &events.Snoopy{
			Syslog: s,
			Snoopy: *val,
		}, nil
	case atomic.Snoopy:
		return &events.Snoopy{
			Syslog: s,
			Snoopy: val,
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
