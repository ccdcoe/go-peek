package process

import (
	"errors"
	"go-peek/pkg/models/atomic"
	"go-peek/pkg/models/events"
	"time"

	"github.com/influxdata/go-syslog/v3"
	"github.com/influxdata/go-syslog/v3/rfc5424"
)

var (
	ErrInvalidSyslogType = errors.New("Not RFC5424")
)

type ErrUnsupportedEventType struct {
	Data []byte
}

func (e ErrUnsupportedEventType) Error() string {
	return "Unsupported event: " + string(e.Data)
}

type Normalizer struct {
	RFC5424 syslog.Machine
}

func (n Normalizer) NormalizeSyslog(data []byte) (interface{}, error) {
	msg, err := parseRFC5424(data, n)
	if err != nil {
		return nil, err
	}
	payload, err := atomic.ParseSyslogMessage(*msg)
	if err != nil {
		return nil, err
	}
	switch val := payload.(type) {
	case *atomic.Snoopy:
		return &events.Snoopy{
			Syslog: *msg,
			Snoopy: *val,
		}, nil
	case atomic.Syslog, *atomic.Syslog:
		return &events.Syslog{
			Syslog: *msg,
		}, nil
	default:
		return msg, ErrUnsupportedEventType{Data: data}
	}
}

func parseRFC5424(data []byte, n Normalizer) (*atomic.Syslog, error) {
	msg, err := n.RFC5424.Parse(data)
	if err != nil {
		return nil, err
	}
	switch v := msg.(type) {
	case *rfc5424.SyslogMessage:
		return &atomic.Syslog{
			Timestamp: checkTime(v.Timestamp),
			Facility:  checkStr(v.FacilityMessage()),
			Host:      checkStr(v.Hostname),
			Program:   checkStr(v.Appname),
			Severity:  checkStr(v.SeverityLevel()),
			Message:   checkStr(v.Message),
		}, nil
	default:
		return nil, ErrInvalidSyslogType
	}
}

func NewNormalizer() *Normalizer {
	return &Normalizer{
		RFC5424: rfc5424.NewParser(rfc5424.WithBestEffort()),
	}
}

func checkStr(in *string) string {
	if in != nil {
		return *in
	}
	return ""
}

func checkTime(in *time.Time) time.Time {
	if in != nil {
		return *in
	}
	return time.Now()
}
