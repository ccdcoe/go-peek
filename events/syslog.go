package events

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

type Syslog struct {
	Timestamp time.Time `json:"@timestamp"`
	Host      string    `json:"host"`
	Program   string    `json:"program"`
	Severity  string    `json:"severity"`
	Facility  string    `json:"facility"`
	IP        *stringIP `json:"ip"`
	Message   string    `json:"message"`

	GameMeta *GameMeta `json:"gamemeta,omitempty"`
}

func (s Syslog) JSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s Syslog) Source() Source {
	return Source{
		Host: s.Host,
		IP:   s.IP.String(),
	}
}

func (s *Syslog) Rename(pretty string) {
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
func (s Syslog) SaganString() string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s",
		s.IP.String(),
		s.Facility,
		s.Severity,
		s.Severity,
		s.Program,
		s.GetSyslogTime().Format(saganDateFormat),
		s.GetSyslogTime().Format(saganTimeFormat),
		s.Program,
		s.Message,
	)
}

func (s *Syslog) Meta(topic, iter string) Event {
	s.GameMeta = &GameMeta{
		Iter:  iter,
		Topic: topic,
	}
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
