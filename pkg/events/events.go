package events

import (
	"fmt"
	"time"
)

const (
	saganDateFormat = "2006-01-02"
	saganTimeFormat = "15:04:05"
)

type ErrTopicNotSupported struct {
	topic   string
	message []byte
}

func (e ErrTopicNotSupported) Error() string {
	if e.topic == "" {
		return fmt.Sprintf("topic missing from message, make sure it was copied properly in pipeline. Msg is %s",
			string(e.message))
	}
	return fmt.Sprintf("%s not supported by event switch. Data is %s",
		e.topic, string(e.message))
}

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
		return nil, &ErrTopicNotSupported{
			topic:   topic,
			message: payload,
		}
	}
}
