package events

import (
	"encoding/json"
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
	return s.Timestamp
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
