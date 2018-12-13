package events

import (
	"encoding/json"
	"fmt"
	"time"
)

const saganDateFormat = "2006-01-02"
const saganTimeFormat = "15:04:05"

type Event interface {
	EventReporter
	EventSourceModifier
	EventFormatter
	EventTimer
}

type EventReporter interface {
	Key() string
}

type EventSourceModifier interface {
	Source() *Source
	Rename(string)
}

type EventFormatter interface {
	JSON() ([]byte, error)
	SaganString() (string, error)
}

type EventTimer interface {
	GetEventTime() time.Time
	GetSyslogTime() time.Time
}

func NewEvent(topic string, payload []byte) (Event, error) {

	switch topic {
	case "syslog":
		var m Syslog
		if err := json.Unmarshal(payload, &m); err != nil {
			return nil, err
		}
		return &m, nil

	case "snoopy":
		var m Snoopy
		if err := json.Unmarshal(payload, &m); err != nil {
			return nil, err
		}
		return &m, nil

	case "suricata":
		var m Eve
		if err := json.Unmarshal(payload, &m); err != nil {
			return nil, err
		}
		return &m, nil

	case "eventlog":
		return NewDynaEventLog(payload)

	default:
		return nil, fmt.Errorf("Unsupported topic %s",
			topic)
	}
}
