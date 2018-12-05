package events

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/Jeffail/gabs"
)

const eventLogTsFormatSlash = "2006/01/02 15:04:05"
const eventLogTsFormatDash = "2006-01-02 15:04:05"

type eventLogTs struct{ time.Time }

func (t *eventLogTs) UnmarshalJSON(b []byte) error {
	raw, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	t.Time, err = time.Parse(eventLogTsFormatSlash, raw)
	return err
}

//type DynaEventLog struct{ Vals *gabs.Container }
type DynaEventLog struct {
	Vals      *gabs.Container
	Timestamp time.Time
	EventTime time.Time
	GameMeta  *GameMeta `json:"gamemeta,omitempty"`
}

func NewDynaEventLog(raw []byte) (*DynaEventLog, error) {
	var err error
	var e = &DynaEventLog{}
	var tsFormat = eventLogTsFormatDash
	if e.Vals, err = gabs.ParseJSON(raw); err != nil {
		return nil, err
	}
	switch v := e.Vals.Path("SourceName").Data().(string); {
	case v == "Microsoft-Windows-Sysmon":
		e.EventTime, err = time.Parse(tsFormat, e.getStringField("EventReceivedTime"))
		if err != nil {
			fmt.Println(e.getStringField("Channel"))
			return nil, err
		}
	default:
		field := e.getStringField("EventTime")
		e.EventTime, err = time.Parse(tsFormat, field)
		if err != nil {
			fmt.Println(e.Vals.String())
			return nil, err
		}
	}
	// TODO! Get syslog time in addition to messed up reported
	e.Timestamp = e.EventTime

	return e, nil
}

func (s DynaEventLog) getStringField(key string) string {
	return s.Vals.Path(key).Data().(string)
}

func (s *DynaEventLog) parseTime(format string) error {
	raw, err := strconv.Unquote(s.Vals.Path("EventReceivedTime").Data().(string))
	if err != nil {
		return err
	}
	if s.EventTime, err = time.Parse(format, raw); err != nil {
		return err
	}
	return nil
}
func (s DynaEventLog) JSON() ([]byte, error) {
	return s.Vals.Bytes(), nil
}

func (s DynaEventLog) Source() Source {
	var (
		name = s.Vals.Path("Hostname").Data().(string)
		ip   = s.Vals.Path("ip").Data().(string)
	)
	return Source{
		Host: name,
		IP:   ip,
	}
}

func (s *DynaEventLog) Rename(pretty string) {
	s.Vals.Set(pretty, "host")
	s.Vals.Set(pretty, "Hostname")
	//s.Vals.DeleteP("ip")
}

func (s DynaEventLog) Key() string {
	return s.Vals.Path("SourceName").Data().(string)
}

func (s DynaEventLog) GetEventTime() time.Time {
	return s.EventTime
}
func (s DynaEventLog) GetSyslogTime() time.Time {
	return s.Timestamp
}

func (s DynaEventLog) SaganString() string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s",
		s.getStringField("ip"),
		s.getStringField("SourceName"),
		s.getStringField("Severity"),
		s.getStringField("Severity"),
		s.getStringField("Channel"),
		s.GetSyslogTime().Format(saganDateFormat),
		s.GetSyslogTime().Format(saganTimeFormat),
		s.getStringField("program"),
		s.getStringField("Message"),
	)
}

func (s *DynaEventLog) Meta(topic, iter string) Event {
	s.GameMeta = &GameMeta{
		Iter:  iter,
		Topic: topic,
	}
	return s
}

type SimpleEventLog struct {
	Syslog

	EventTime  *eventLogTs `json:"EventTime"`
	Channel    string      `json:"Channel"`
	Hostname   string      `json:"Hostname"`
	SourceName string      `json:"SourceName"`
}

func (s SimpleEventLog) JSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s SimpleEventLog) Source() Source {
	return Source{
		Host: s.Hostname,
		IP:   s.Host,
	}
}

func (s *SimpleEventLog) Rename(pretty string) {
	s.Host = pretty
	s.Hostname = pretty
}

func (s SimpleEventLog) GetEventTime() time.Time {
	return s.EventTime.Time
}
