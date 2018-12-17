package events

import (
	"fmt"
	"time"
)

const (
	saganDateFormat = "2006-01-02"
	saganTimeFormat = "15:04:05"
)

type Event interface {
	EventReporter
	EventSourceModifier
	EventFormatter
	EventTimeReporter
}

type EventReporter interface {
	Key() string
}

type EventTimeReporter interface {
	GetEventTime() time.Time
	GetSyslogTime() time.Time
}

type EventSourceModifier interface {
	Source() (*Source, error)
	Rename(string)
}

type EventFormatter interface {
	JSON() ([]byte, error)
	SaganString() (string, error)
}

func NewEvent(topic string, payload []byte) (Event, error) {

	switch topic {
	case "syslog":
		return NewSyslog(payload)
	case "snoopy":
		return NewSnoopy(payload)
	case "suricata":
		return NewEVE(payload)
	case "eventlog":
		return NewDynaEventLog(payload)
	default:
		// *TODO* This doesn't have to fail.
		// We can use a generic gabs / map[string]interface{} with time.now instead
		return nil, fmt.Errorf("Unsupported topic %s",
			topic)
	}
}
