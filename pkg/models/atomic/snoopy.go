package atomic

import (
	"fmt"
	"net"
	"strings"
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

type ErrParseSnoopy struct {
	Msg    string
	Reason string
}

func (e ErrParseSnoopy) Error() string {
	return fmt.Sprintf("Unable to parse snoopy msg [%s] BECAUSE: %s", e.Msg, e.Reason)
}

func ParseSnoopy(m string) (*Snoopy, error) {
	if strings.HasPrefix(m, " ") {
		m = strings.TrimLeft(m, " ")
	}
	if !strings.HasPrefix(m, "[") {
		return nil, ErrParseSnoopy{Msg: m, Reason: "Invalid header"}
	}
	c := strings.Index(m, "]")
	if c == -1 {
		return nil, ErrParseSnoopy{Msg: m, Reason: "No closing bracket"}
	}
	sub := false
	f := func(c rune) bool {
		switch {
		case c == '(':
			sub = !sub
			return false
		case c == ')':
			sub = !sub
			return true
		case sub:
			return false
		default:
			return c == ' '
		}
	}
	data := strings.TrimLeft(m[:c], "[")
	cmd := m[c:]
	obj := &Snoopy{}
	if i := strings.Index(cmd, ":"); i != -1 && len(cmd) > 3 {
		obj.Cmd = strings.TrimRight(cmd[i+1:], "\n")
	}

	//fields := strings.Fields(data)
	items := strings.FieldsFunc(data, f)
	switch len(items) {
	case 10:
		// custom config with extended info
		bites := make([]string, 10)
		for i, f := range items {
			if val := strings.SplitN(f, ":", 2); len(val) != 2 {
				continue
			} else if i == 1 && strings.HasPrefix(f, "ssh") {
				if cuts := strings.Split(f, "("); len(cuts) == 2 {
					if cuts = strings.Fields(cuts[1]); len(cuts) == 4 {
						obj.SSH = &SnoopySSH{
							SrcIP:   &fields.StringIP{IP: net.ParseIP(cuts[0])},
							SrcPort: cuts[1],
							DstIP:   &fields.StringIP{IP: net.ParseIP(cuts[2])},
							DstPort: cuts[3],
						}
					}
				}
			} else {
				bites[i] = val[1]
			}
			obj.Login = bites[0]
			obj.Username = bites[2]
			obj.UID = bites[3]
			obj.Group = bites[4]
			obj.Gid = bites[5]
			obj.Sid = bites[6]
			obj.Tty = bites[7]
			obj.Cwd = bites[8]
			obj.Filename = bites[9]
		}
	case 5:
		// default config
		bites := make([]string, 5)
		for i, f := range items {
			if val := strings.SplitN(f, ":", 2); len(val) == 2 {
				bites[i] = val[1]
			}
		}
		obj.UID = bites[0]
		obj.Sid = bites[1]
		obj.Tty = strings.TrimLeft(bites[2], "(")
		obj.Cwd = bites[3]
		obj.Filename = bites[4]
	default:
		return nil, ErrParseSnoopy{Msg: m}
	}

	return obj, nil
}
