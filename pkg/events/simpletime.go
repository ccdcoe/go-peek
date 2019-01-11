package events

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/ccdcoe/go-peek/internal/types"
)

type eventTimeStamp struct{ time.Time }

func (t *eventTimeStamp) UnmarshalJSON(b []byte) error {
	raw, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	t.Time, err = time.Parse(eventLogTsFormatDash, raw)
	return err
}

type eventReceivedTimeStamp struct{ time.Time }

func (t *eventReceivedTimeStamp) UnmarshalJSON(b []byte) error {
	raw, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	t.Time, err = time.Parse(eventLogTsFormatDash, raw)
	return err
}

// SimpleTime is only for quickly grabbing the timestamp from messages without knowing the message content beforehand
// Meant for multi-pass approaches where quick timestamp parse is desireable on first pass, so the information can be used during second
// All known Timestamp keys will be tried
// SimpleTime implements Event for compliance, but only timestamp methods should be used
type SimpleTime struct {
	Timestamp         time.Time               `json:"@timestamp,omitempty"`
	SuriTimestamp     *suriTS                 `json:"timestamp,omitempty"`
	EventTime         *eventTimeStamp         `json:"EventTime,omitempty"`
	EventReceivedTime *eventReceivedTimeStamp `json:"EventReceivedTime,omitempty"`
}

func (s SimpleTime) Key() string {
	panic("not implemented")
}

func (s SimpleTime) Source() (*Source, error) {
	panic("not implemented")
}

func (s *SimpleTime) Rename(string) {
	panic("not implemented")
}

func (s SimpleTime) JSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s SimpleTime) SaganString() (string, error) {
	return "", &types.ErrNotImplemented{Err: fmt.Errorf("Not implemented")}
}

func (s SimpleTime) GetEventTime() time.Time {
	if s.SuriTimestamp != nil {
		return s.SuriTimestamp.Time
	}
	if s.EventReceivedTime != nil {
		return s.EventReceivedTime.Time
	}
	if s.EventTime != nil {
		return s.EventTime.Time
	}
	return s.Timestamp
}

func (s SimpleTime) GetSyslogTime() time.Time {
	return s.Timestamp
}
