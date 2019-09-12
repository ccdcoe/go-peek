package atomic

import (
	"time"

	"github.com/ccdcoe/go-peek/pkg/models/fields"
)

type Snoopy struct {
	Cmd      string     `json:"cmd,omitempty"`
	Filename string     `json:"filename,omitempty"`
	Cwd      string     `json:"cwd,omitempty"`
	Tty      string     `json:"tty,omitempty"`
	Sid      string     `json:"sid,omitempty"`
	Gid      string     `json:"gid,omitempty"`
	Group    string     `json:"group,omitempty"`
	UID      string     `json:"uid,omitempty"`
	Username string     `json:"username,omitempty"`
	Login    string     `json:"login,omitempty"`
	SSH      *SnoopySSH `json:"ssh,omitempty"`
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (s Snoopy) Time() time.Time { return time.Time{} }

// Source implements atomic.Event
// Source of message, usually emitting program
func (s Snoopy) Source() string { return s.Username }

// Sender implements atomic.Event
// Sender of message, usually a host
func (s Snoopy) Sender() string { return s.Cmd }

type SnoopySSH struct {
	DstPort string           `json:"dst_port,omitempty"`
	DstIP   *fields.StringIP `json:"dst_ip,omitempty"`
	SrcPort string           `json:"src_port,omitempty"`
	SrcIP   *fields.StringIP `json:"src_ip,omitempty"`
}

func (s SnoopySSH) Empty() bool {
	if s.DstIP == nil && s.DstPort == "" && s.SrcIP == nil && s.SrcPort == "" {
		return true
	}
	return false
}
