package events

import (
	"strconv"
	"time"
)

const (
	suriTsFmt     = "2006-01-02T15:04:05.999999-0700"
	eventLogTsFmt = "2006-01-02 15:04:05"
)

var (
	AlwaysUseSyslogTimestamp = false
)

type suricataTimeStamp struct{ time.Time }

func (t *suricataTimeStamp) UnmarshalJSON(b []byte) (err error) {
	t.Time, err = parseTimeFromByte(b, suriTsFmt)
	return err
}

type eventTimeStamp struct{ time.Time }

func (t *eventTimeStamp) UnmarshalJSON(b []byte) (err error) {
	t.Time, err = parseTimeFromByte(b, eventLogTsFmt)
	return err
}

type eventReceivedTimeStamp struct{ time.Time }

func (t *eventReceivedTimeStamp) UnmarshalJSON(b []byte) (err error) {
	t.Time, err = parseTimeFromByte(b, eventLogTsFmt)
	return err
}

func parseTimeFromByte(b []byte, fmt string) (time.Time, error) {
	raw, err := strconv.Unquote(string(b))
	if err != nil {
		return time.Now(), err
	}
	t, err := time.Parse(fmt, raw)
	return t, err
}

type KnownTimeStamps struct {
	Timestamp         time.Time               `json:"@timestamp,omitempty"`
	SuriTimestamp     *suricataTimeStamp      `json:"timestamp,omitempty"`
	EventTime         *eventTimeStamp         `json:"EventTime,omitempty"`
	EventReceivedTime *eventReceivedTimeStamp `json:"EventReceivedTime,omitempty"`
}

func (s KnownTimeStamps) Time() time.Time {
	if AlwaysUseSyslogTimestamp {
		return s.Timestamp
	}
	if s.EventReceivedTime != nil {
		return s.EventReceivedTime.Time
	}
	if s.EventTime != nil {
		return s.EventTime.Time
	}
	if s.SuriTimestamp != nil {
		return s.SuriTimestamp.Time
	}
	return s.Timestamp
}
