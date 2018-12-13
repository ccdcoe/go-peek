package events

import (
	"fmt"
	"strings"
	"time"

	"github.com/Jeffail/gabs"
)

const (
	eventLogTsFormatDash = "2006-01-02 15:04:05"
	eventLogSourceKey    = "SourceName"
	eventLogHostKey      = "Hostname"
	eventLogSevKey       = "Severity"
	eventLogChanKey      = "Channel"
	eventLogMsgKey       = "Message"

	eventLogSysmonTsKey = "EventReceivedTime"
	eventLogTsKey       = "EventTime"

	syslogHostKey  = "syslog_host"
	syslogIPKey    = "syslog_ip"
	syslogSevKey   = "syslog_severity"
	syslogProgKey  = "syslog_program"
	syslogTsKey    = "@timestamp"
	syslogTsFormat = time.RFC3339Nano
)

type DynaEventLog struct {
	Vals *gabs.Container

	Timestamp time.Time `json:"@timestamp"`
	EventTime time.Time `json:"event_time"`

	GameMeta *Source `json:"gamemeta"`
}

func NewDynaEventLog(raw []byte) (*DynaEventLog, error) {
	var (
		err      error
		src      string
		e        = &DynaEventLog{}
		tsFormat = eventLogTsFormatDash
	)
	if e.Vals, err = gabs.ParseJSON(raw); err != nil {
		return nil, &EventParseErr{key: "full", err: err, raw: raw}
	}

	if src, err = e.getStringFieldWithErr(eventLogSourceKey); err != nil {
		return nil, &EventParseErr{key: eventLogSourceKey, err: err, raw: raw}
	}
	switch src {
	case "Microsoft-Windows-Sysmon":
		if e.EventTime, err = e.parseTimeFromGabInterface(
			eventLogSysmonTsKey,
			tsFormat,
		); err != nil {
			return e, err
		}
	default:
		if e.EventTime, err = e.parseTimeFromGabInterface(
			eventLogTsKey,
			tsFormat,
		); err != nil {
			return e, err
		}
	}

	if e.EventTime, err = e.parseTimeFromGabInterface(
		syslogTsKey,
		syslogTsFormat,
	); err != nil {
		return e, err
	}

	if err = e.setDefaultEventShipperSource(
		syslogHostKey,
		syslogIPKey,
	); err != nil {
		return e, err
	}

	return e, nil
}

func (s *DynaEventLog) JSON() ([]byte, error) {
	s.Vals.SetP(s.Timestamp, syslogTsKey)
	s.Vals.SetP(s.GameMeta, "gamemeta")
	return s.Vals.Bytes(), nil
}

func (s *DynaEventLog) Source() *Source {
	return s.GameMeta
}

func (s *DynaEventLog) Rename(pretty string) {
	s.Vals.Set(pretty, syslogHostKey)
	s.Vals.Set(pretty, eventLogHostKey)
}

func (s DynaEventLog) Key() string {
	return s.Vals.Path(eventLogSourceKey).Data().(string)
}

func (s DynaEventLog) GetEventTime() time.Time {
	return s.EventTime
}
func (s DynaEventLog) GetSyslogTime() time.Time {
	return s.Timestamp
}

func (s DynaEventLog) SaganString() (string, error) {
	var keys = []string{
		syslogTsKey,
		eventLogSourceKey,
		eventLogSevKey,
		eventLogSevKey,
		eventLogChanKey,
		syslogProgKey,
		eventLogMsgKey,
	}
	var (
		vals = make([]string, len(keys))
		err  error
	)
	for i, v := range keys {
		if vals[i], err = s.getStringFieldWithErr(v); err != nil {
			return "", err
		}
	}
	return strings.Join(
		[]string{
			keys[0],
			keys[1],
			keys[2],
			keys[3],
			keys[4],
			s.GetSyslogTime().Format(saganDateFormat),
			s.GetSyslogTime().Format(saganTimeFormat),
			keys[5],
			keys[6],
		}, "|",
	), nil
}

func (s DynaEventLog) getStringField(key string) string {
	return s.Vals.Path(key).Data().(string)
}

func (s DynaEventLog) getStringFieldWithErr(key string) (string, error) {
	if !s.Vals.ExistsP(key) {
		return "", fmt.Errorf("key %s does not exist in doc", key)
	}
	val := s.Vals.Path(key).Data()
	switch val.(type) {
	case string:
		return val.(string), nil
	default:
		return "", fmt.Errorf("key %s value not string", key)
	}
}

func (s *DynaEventLog) setDefaultEventShipperSource(hostkey, ipkey string) (err error) {
	if s.GameMeta == nil {
		s.GameMeta = &Source{}
	}
	if s.GameMeta.Host == "" {
		if s.GameMeta.Host, err = s.getStringFieldWithErr(hostkey); err != nil {
			return err
		}
	}
	if s.GameMeta.IP == "" {
		if s.GameMeta.IP, err = s.getStringFieldWithErr(ipkey); err != nil {
			return err
		}
	}
	return nil
}

func (s DynaEventLog) parseTimeFromGabInterface(key, format string) (time.Time, error) {
	var (
		stringval string
		timestamp time.Time
		err       error
	)
	if stringval, err = s.getStringFieldWithErr(syslogTsKey); err != nil {
		return time.Now(), &EventParseErr{key: syslogTsKey, err: err, raw: s.Vals.Bytes()}
	}
	if timestamp, err = time.Parse(time.RFC3339Nano, stringval); err != nil {
		return time.Now(), &EventParseErr{key: stringval, err: err, raw: s.Vals.Bytes()}
	}
	return timestamp, nil
}
