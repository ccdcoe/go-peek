package events

import (
	"encoding/json"
	"fmt"
)

type Source struct {
	Host, IP string
}

type Event interface {
	JSON() ([]byte, error)
	Source() Source
	Rename(string)
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
		switch m.EventType {
		case "alert":
			return &m, nil
		}

	case "eventlog":
		var m EventLog
		if err := json.Unmarshal(payload, &m); err != nil {
			return nil, err
		}
		return &m, nil

	default:
		return nil, fmt.Errorf("Unsupported topic %s",
			topic)
	}
	return nil, nil
}
