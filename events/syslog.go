package events

import (
	"encoding/json"
	"net"
	"strings"
	"time"
)

type Syslog struct {
	Timestamp time.Time `json:"@timestamp"`
	Host      string    `json:"syslog_host"`
	Program   string    `json:"syslog_program"`
	Severity  string    `json:"syslog_severity"`
	Facility  string    `json:"syslog_facility"`
	IP        *stringIP `json:"syslog_ip"`
	Message   string    `json:"syslog_message"`

	GameMeta *Source `json:"gamemeta"`
}

func (s Syslog) JSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s *Syslog) Source() *Source {
	if s.GameMeta == nil {
		s.GameMeta = &Source{Host: s.Host, IP: s.IP.String()}
	}
	return s.GameMeta
}

func (s *Syslog) Rename(pretty string) {
	if s.GameMeta != nil {
		s.GameMeta.Host = pretty
	}
	s.Host = pretty
}

func (s Syslog) Key() string {
	return s.Program
}

func (s Syslog) GetEventTime() time.Time {
	return s.GetSyslogTime()
}
func (s Syslog) GetSyslogTime() time.Time {
	return s.Timestamp
}
func (s Syslog) SaganString() (string, error) {
	return strings.Join(
		[]string{
			s.IP.String(),
			s.Facility,
			s.Severity,
			s.Severity,
			s.Program,
			s.GetSyslogTime().Format(saganDateFormat),
			s.GetSyslogTime().Format(saganTimeFormat),
			s.Program,
			s.Message,
		}, "|",
	), nil
}

func (s *Syslog) Meta() Event {
	return s
}

// NewSyslogTestMessage is a helper function for usage in _test.go files
func NewSyslogTestMessage(host string) *Syslog {
	if host == "" {
		host = "my.awesome.server"
	}
	return &Syslog{
		Timestamp: time.Now(),
		Host:      host,
		Program:   "some/dumb-app",
		Severity:  "info",
		Facility:  "daemon",
		IP:        &stringIP{IP: net.ParseIP("12.3.4.5")},
		Message:   "[this dev has no idea how syslog works] this app is really messed up",
	}
}
