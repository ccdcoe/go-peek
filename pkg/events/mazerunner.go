package events

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/ccdcoe/go-peek/internal/types"
)

type MazeRunner struct {
	Syslog

	Cef Cef `json:"cef,omitempty"`

	GameMeta *Source `json:"gamemeta,omitempty"`
}

func NewMazeRunner(raw []byte) (*MazeRunner, error) {
	var m = &MazeRunner{}
	if err := json.Unmarshal(raw, m); err != nil {
		return nil, err
	}
	m.Timestamp = m.GetEventTime()
	if _, err := m.Source(); err != nil {
		return m, err
	}
	return m, nil
}

func (m MazeRunner) Key() string {
	return ""
}

func (m *MazeRunner) Source() (*Source, error) {
	if m.GameMeta == nil {
		m.GameMeta = NewSource()
	}

	if m.Syslog.IP != nil {
		m.GameMeta.IP = m.Syslog.IP.IP
	}

	if val, ok := m.Cef.Extensions["src"]; ok {
		ip := net.ParseIP(val)
		if ip == nil {
			return nil, &ErrParseIP{
				ip:    val,
				event: m,
			}
		}
		m.GameMeta.SetSrcIp(ip)
	}

	if val, ok := m.Cef.Extensions["dst"]; ok {
		ip := net.ParseIP(val)
		if ip == nil {
			return nil, &ErrParseIP{
				ip:    val,
				event: m,
			}
		}
		m.GameMeta.SetDestIp(ip)
	}

	return m.GameMeta, nil
}

func (m *MazeRunner) Rename(pretty string) {
	m.Syslog.Host = pretty
}

func (m MazeRunner) JSON() ([]byte, error) {
	return json.Marshal(m)
}

func (m *MazeRunner) SaganString() (string, error) {
	return "NOT IMPLEMENTED", &types.ErrNotImplemented{
		Err: fmt.Errorf(
			"SaganString method not implemented for mazerunner event",
		),
	}
}

func (m MazeRunner) GetEventTime() time.Time {
	return m.Syslog.Timestamp
}

func (m MazeRunner) GetSyslogTime() time.Time {
	return m.Syslog.Timestamp
}

type Cef struct {
	DeviceVendor  string            `json:"DeviceVendor"`
	DeviceProduct string            `json:"DeviceProduct"`
	DeviceVersion string            `json:"DeviceVersion"`
	SignatureID   string            `json:"SignatureID"`
	Name          string            `json:"Name"`
	Severity      string            `json:"Severity"`
	Extensions    map[string]string `json:"Extensions"`
}
