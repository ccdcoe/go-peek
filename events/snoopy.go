package events

import (
	"encoding/json"
	"strings"
	"time"
)

type Snoopy struct {
	Syslog

	Cmd      string `json:"cmd"`
	Filename string `json:"filename"`
	Cwd      string `json:"cwd"`
	Tty      string `json:"tty"`
	Sid      string `json:"sid"`
	Gid      string `json:"gid"`
	Group    string `json:"group"`
	UID      string `json:"uid"`
	Username string `json:"username"`
	SSH      struct {
		DstPort string `json:"dst_port"`
		DstIP   string `json:"dst_ip"`
		SrcPort string `json:"src_port"`
		SrcIP   string `json:"src_ip"`
	} `json:"ssh"`
	Login string `json:"login"`

	GameMeta *Source `json:"gamemeta"`
}

func NewSnoopy(raw []byte) (*Snoopy, error) {
	var s = &Snoopy{}
	if err := json.Unmarshal(raw, s); err != nil {
		return nil, err
	}
	return s, nil
}

func (s Snoopy) JSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s *Snoopy) Source() (*Source, error) {
	if s.GameMeta == nil {
		s.GameMeta = &Source{
			Host: s.Host,
			IP:   s.IP.String(),
		}
	}
	return s.GameMeta, nil
}

func (s *Snoopy) Rename(pretty string) {
	if s.GameMeta != nil {
		s.GameMeta.Host = pretty
	}
	s.Host = pretty
}

func (s Snoopy) Key() string {
	return s.Filename
}

func (s Snoopy) EventTime() time.Time {
	return s.Timestamp
}
func (s Snoopy) GetSyslogTime() time.Time {
	return s.Timestamp
}
func (s Snoopy) SaganString() (string, error) {
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
			s.Cmd,
		}, "|",
	), nil
}
