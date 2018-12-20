package events

import (
	"encoding/json"
	"strings"
	"time"
)

type Snoopy struct {
	Syslog

	Cmd      string `json:"cmd,omitempty"`
	Filename string `json:"filename,omitempty"`
	Cwd      string `json:"cwd,omitempty"`
	Tty      string `json:"tty,omitempty"`
	Sid      string `json:"sid,omitempty"`
	Gid      string `json:"gid,omitempty"`
	Group    string `json:"group,omitempty"`
	UID      string `json:"uid,omitempty"`
	Username string `json:"username,omitempty"`
	Login    string `json:"login,omitempty"`

	SSH      *SnoopySSH `json:"ssh,omitempty"`
	GameMeta *Source    `json:"gamemeta,omitempty"`
}

type SnoopySSH struct {
	DstPort string    `json:"dst_port,omitempty"`
	DstIP   *stringIP `json:"dst_ip,omitempty"`
	SrcPort string    `json:"src_port,omitempty"`
	SrcIP   *stringIP `json:"src_ip,omitempty"`
}

func (s SnoopySSH) Empty() bool {
	if s.DstIP == nil && s.DstPort == "" && s.SrcIP == nil && s.SrcPort == "" {
		return true
	}
	return false
}

func NewSnoopy(raw []byte) (*Snoopy, error) {
	var s = &Snoopy{}
	if err := json.Unmarshal(raw, s); err != nil {
		return nil, err
	}
	return s.setMeta(), nil
}

func (s Snoopy) JSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s *Snoopy) Source() (*Source, error) {
	if s.GameMeta == nil {
		s.setMeta()
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

func (s *Snoopy) setMeta() *Snoopy {
	s.GameMeta = NewSource()
	s.GameMeta.SetHost(s.Host).SetIp(s.IP.IP)

	if s.SSH != nil && !s.SSH.Empty() {
		if len(s.SSH.SrcIP.IP) > 0 {
			s.GameMeta.SetSrcIp(s.SSH.SrcIP.IP)
		}
		if len(s.SSH.DstIP.IP) > 0 {
			s.GameMeta.SetDestIp(s.SSH.DstIP.IP)
		}
	}
	return s
}
