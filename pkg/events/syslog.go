package events

import (
	"encoding/json"
	"net"
	"strings"
	"time"

	"github.com/ccdcoe/go-peek/internal/types"
)

type Syslog struct {
	Timestamp time.Time       `json:"@timestamp"`
	Host      string          `json:"syslog_host"`
	Program   string          `json:"syslog_program"`
	Severity  string          `json:"syslog_severity"`
	Facility  string          `json:"syslog_facility"`
	IP        *types.StringIP `json:"syslog_ip,omitempty"`
	Message   string          `json:"syslog_message"`

	GameMeta *Source `json:"gamemeta"`
}

func NewSyslog(raw []byte) (*Syslog, error) {
	var s = &Syslog{}
	if err := json.Unmarshal(raw, s); err != nil {
		return nil, &ErrDecode{
			raw: raw,
			err: err,
		}
	}
	return s.setMeta(), nil
}

func (s Syslog) JSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s *Syslog) Source() (*Source, error) {
	if s.GameMeta == nil {
		s.setMeta()
	}
	return s.GameMeta, nil
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
			s.GetSrcString(),
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

func (s Syslog) GetSrcString() string {
	if s.IP != nil {
		return s.IP.String()
	}
	return ""
}

func (s *Syslog) setMeta() *Syslog {
	s.GameMeta = NewSource()
	s.GameMeta.SetHost(s.Host)
	if s.IP != nil {
		s.GameMeta.SetIp(s.IP.IP)
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
		IP:        &types.StringIP{IP: net.ParseIP("12.3.4.5")},
		Message:   "[this dev has no idea how syslog works] this app is really messed up",
	}
}
