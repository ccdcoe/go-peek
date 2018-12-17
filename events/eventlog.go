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

	Host      string    `json:"host"`
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

	if e.Host, err = e.getStringFieldWithErr(eventLogHostKey); err != nil {
		return nil, &EventParseErr{key: eventLogHostKey, err: err, raw: raw}
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
	if s.GameMeta != nil {
		s.GameMeta.Host = s.Host
	}
	//s.Vals.SetP(s.Timestamp, syslogTsKey)
	s.Vals.SetP(s.GameMeta, "gamemeta")
	return s.Vals.Bytes(), nil
}

func (s *DynaEventLog) Source() (*Source, error) {
	return s.GameMeta, nil
}

func (s *DynaEventLog) Rename(pretty string) {
	s.Vals.Set(pretty, syslogHostKey)
	s.Vals.Set(pretty, eventLogHostKey)
	s.Host = pretty
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
		syslogIPKey,
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

func (s DynaEventLog) checkAndGetUntypedField(key string) (interface{}, error) {
	if !s.Vals.ExistsP(key) {
		return "", fmt.Errorf("key %s does not exist in doc", key)
	}
	return s.Vals.Path(key).Data(), nil
}

func (s DynaEventLog) getStringFieldWithErr(key string) (string, error) {
	var (
		val interface{}
		err error
	)
	if val, err = s.checkAndGetUntypedField(key); err != nil {
		return "", err
	}
	switch val.(type) {
	case string:
		return val.(string), nil
	default:
		return "", fmt.Errorf("key %s value not string", key)
	}
}
func (s DynaEventLog) parseTimeFromGabInterface(key, format string) (time.Time, error) {
	var (
		val       interface{}
		timestamp time.Time
		err       error
	)
	if val, err = s.checkAndGetUntypedField(key); err != nil {
		return time.Now(), err
	}
	switch v := val.(type) {
	case time.Time:
		return v, nil
	case string:
		if timestamp, err = time.Parse(format, v); err != nil {
			return time.Now(), &EventParseErr{key: v, err: err, raw: s.Vals.Bytes()}
		}
		return timestamp, nil
	default:
		return time.Now(), &EventParseErr{
			key: key,
			err: fmt.Errorf("unknown type for key %s, expecting string or time", key),
		}
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
