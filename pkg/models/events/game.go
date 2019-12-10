package events

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/ccdcoe/go-peek/pkg/models/atomic"
	"github.com/ccdcoe/go-peek/pkg/models/fields"
	"github.com/ccdcoe/go-peek/pkg/models/meta"
	"github.com/influxdata/go-syslog/rfc5424"
)

type GameEvent interface {
	atomic.Event
	meta.AssetGetterSetter
	atomic.JSONFormatter
}

type ErrEventParse struct {
	Data   []byte
	Wanted Atomic
	Reason string
	Err    error
}

func (e ErrEventParse) Error() string {
	return fmt.Sprintf(
		"Problem parsing event [%s] as %s. Reason: %s",
		string(e.Data),
		e.Wanted,
		e.Reason,
	)
}

func ParseSyslog(data []byte, enum Atomic) (GameEvent, error) {
	bestEffort := true
	msg, err := rfc5424.NewParser().Parse(data, &bestEffort)
	if err != nil {
		return nil, err
	}
	s := atomic.Syslog{
		Timestamp: *msg.Timestamp(),
		Message:   *msg.Message(),
		Host:      *msg.Hostname(),
		Program:   *msg.Appname(),
	}
	switch enum {
	case SuricataE:
		var obj atomic.StaticSuricataEve
		if err := json.Unmarshal([]byte(s.Message), &obj); err != nil {
			return nil, err
		}
		return &Suricata{
			StaticSuricataEve: obj,
		}, nil
	case EventLogE, SysmonE:
		var obj atomic.DynamicWinlogbeat
		if err := json.Unmarshal([]byte(s.Message), &obj); err != nil {
			return nil, err
		}
		return &DynamicWinlogbeat{
			DynamicWinlogbeat: obj,
		}, nil
	}

	payload, err := atomic.ParseSyslogMessage(s)
	if err != nil {
		return nil, err
	}
	switch val := payload.(type) {
	case *atomic.Snoopy:
		return &Snoopy{
			Syslog: s,
			Snoopy: *val,
		}, nil
	case atomic.Snoopy:
		return &Snoopy{
			Syslog: s,
			Snoopy: val,
		}, nil
	case atomic.Syslog:
		return &Syslog{
			Syslog: s,
		}, nil
	case *atomic.Syslog:
		return &Syslog{
			Syslog: s,
		}, nil
	}
	return nil, fmt.Errorf("Unsupported event type")
}

type Parser int

const (
	ParseJSON Parser = iota
	ParseRFC5424
)

type DynamicWinlogbeat struct {
	atomic.DynamicWinlogbeat
	GameMeta meta.GameAsset `json:"GameMeta,omitempty"`
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (d DynamicWinlogbeat) Time() time.Time { return d.DynamicWinlogbeat.Time() }

// Source implements atomic.Event
// Source of message, usually emitting program
func (d DynamicWinlogbeat) Source() string { return d.DynamicWinlogbeat.Source() }

// Sender implements atomic.Event
// Sender of message, usually a host
func (d DynamicWinlogbeat) Sender() string {
	return d.DynamicWinlogbeat.Sender()
}

// GetAsset is a getter for receiving event source and target information
// For exampe, event source for syslog is usually the shipper, while suricata alert has affected source and destination IP addresses whereas directionality matters
// Should provide needed information for doing external asset table lookups
func (d DynamicWinlogbeat) GetAsset() *meta.GameAsset {
	return &meta.GameAsset{
		Asset: meta.Asset{
			Host: d.Sender(),
			IP:   nil,
		},
		Source:      nil,
		Destination: nil,
	}
}

// SetAsset is a setter for setting meta to object without knowing the object type
// all asset lookups and field discoveries should be done before using this method to maintain readability
func (d *DynamicWinlogbeat) SetAsset(obj meta.GameAsset) {
	d.GameMeta = obj
}

// JSONFormat implements atomic.JSONFormatter by wrapping json.Marshal
func (d DynamicWinlogbeat) JSONFormat() ([]byte, error) {
	return json.Marshal(d)
}

type Suricata struct {
	atomic.StaticSuricataEve
	Syslog   *atomic.Syslog `json:"syslog,omitempty"`
	GameMeta meta.GameAsset `json:"GameMeta,omitempty"`
}

// JSONFormat implements atomic.JSONFormatter by wrapping json.Marshal
func (s Suricata) JSONFormat() ([]byte, error) { return json.Marshal(s) }

// GetAsset is a getter for receiving event source and target information
// For exampe, event source for syslog is usually the shipper, while suricata alert has affected source and destination IP addresses whereas directionality matters
// Should provide needed information for doing external asset table lookups
func (s Suricata) GetAsset() *meta.GameAsset {
	return &meta.GameAsset{
		Asset: meta.Asset{
			Host: s.StaticSuricataEve.Host,
			IP: func() net.IP {
				if s.Syslog == nil || s.Syslog.IP == nil {
					return nil
				}
				return s.Syslog.IP.IP
			}(),
		},
		Source: &meta.Asset{
			IP: func() net.IP {
				if s.StaticSuricataEve.Alert != nil && s.StaticSuricataEve.Alert.Source != nil {
					return s.StaticSuricataEve.Alert.Source.IP.IP
				}
				if s.StaticSuricataEve.SrcIP == nil {
					return nil
				}
				return s.StaticSuricataEve.SrcIP.IP
			}(),
		},
		Destination: &meta.Asset{
			IP: func() net.IP {
				if s.StaticSuricataEve.Alert != nil && s.StaticSuricataEve.Alert.Target != nil {
					return s.StaticSuricataEve.Alert.Target.IP.IP
				}
				if s.StaticSuricataEve.DestIP == nil {
					return nil
				}
				return s.StaticSuricataEve.DestIP.IP
			}(),
		},
	}
}

// SetAsset is a setter for setting meta to object without knowing the object type
// all asset lookups and field discoveries should be done before using this method to maintain readability
func (s *Suricata) SetAsset(data meta.GameAsset) {
	s.GameMeta = data
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (s Suricata) Time() time.Time { return s.StaticSuricataEve.Time() }

// Source implements atomic.Event
// Source of message, usually emitting program
func (s Suricata) Source() string { return s.StaticSuricataEve.Source() }

// Sender implements atomic.Event
// Sender of message, usually a host
func (s Suricata) Sender() string { return s.StaticSuricataEve.Sender() }

type Syslog struct {
	Syslog   atomic.Syslog  `json:"syslog"`
	GameMeta meta.GameAsset `json:"GameMeta,omitempty"`
}

// JSONFormat implements atomic.JSONFormatter by wrapping json.Marshal
func (s Syslog) JSONFormat() ([]byte, error) { return json.Marshal(s) }

// GetAsset is a getter for receiving event source and target information
// For exampe, event source for syslog is usually the shipper, while suricata alert has affected source and destination IP addresses whereas directionality matters
// Should provide needed information for doing external asset table lookups
func (s Syslog) GetAsset() *meta.GameAsset {
	return &meta.GameAsset{
		Directionality: meta.DirLocal,
		Asset: meta.Asset{
			Host: s.Syslog.Host,
			IP: func() net.IP {
				if s.Syslog.IP == nil {
					return nil
				}
				return s.Syslog.IP.IP
			}(),
		},
	}
}

// SetAsset is a setter for setting meta to object without knowing the object type
// all asset lookups and field discoveries should be done before using this method to maintain readability
func (s *Syslog) SetAsset(data meta.GameAsset) {
	s.GameMeta = data
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (s Syslog) Time() time.Time { return s.Syslog.Time() }

// Source implements atomic.Event
// Source of message, usually emitting program
func (s Syslog) Source() string { return s.Syslog.Source() }

// Sender implements atomic.Event
// Sender of message, usually a host
func (s Syslog) Sender() string { return s.Syslog.Sender() }

type Snoopy struct {
	atomic.Snoopy
	Syslog   atomic.Syslog  `json:"syslog"`
	GameMeta meta.GameAsset `json:"GameMeta,omitempty"`
}

// JSONFormat implements atomic.JSONFormatter by wrapping json.Marshal
func (s Snoopy) JSONFormat() ([]byte, error) { return json.Marshal(s) }

// GetAsset is a getter for receiving event source and target information
// For exampe, event source for syslog is usually the shipper, while suricata alert has affected source and destination IP addresses whereas directionality matters
// Should provide needed information for doing external asset table lookups
func (s Snoopy) GetAsset() *meta.GameAsset {
	return &meta.GameAsset{
		Asset: meta.Asset{
			Host: s.Syslog.Host,
			IP: func() net.IP {
				if s.Syslog.IP == nil {
					return nil
				}
				return s.Syslog.IP.IP
			}(),
		},
		Directionality: func() meta.Directionality {
			if s.Snoopy.SSH == nil {
				return meta.DirLocal
			}
			return meta.DirUnk
		}(),
		Source: func() *meta.Asset {
			if s.Snoopy.SSH == nil {
				return nil
			}
			if s.Snoopy.SSH.SrcIP != nil {
				return &meta.Asset{IP: s.Snoopy.SSH.SrcIP.IP}
			}
			return nil
		}(),
		Destination: func() *meta.Asset {
			if s.Snoopy.SSH == nil {
				return nil
			}
			if s.Snoopy.SSH.DstIP != nil {
				return &meta.Asset{IP: s.Snoopy.SSH.DstIP.IP}
			}
			return nil
		}(),
	}
}

// SetAsset is a setter for setting meta to object without knowing the object type
// all asset lookups and field discoveries should be done before using this method to maintain readability
func (s *Snoopy) SetAsset(data meta.GameAsset) {
	s.GameMeta = data
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (s Snoopy) Time() time.Time { return s.Syslog.Time() }

// Source implements atomic.Event
// Source of message, usually emitting program
func (s Snoopy) Source() string { return s.Snoopy.Source() }

// Sender implements atomic.Event
// Sender of message, usually a host
func (s Snoopy) Sender() string { return s.Syslog.Sender() }

type Eventlog struct {
	atomic.EventLog
	GameMeta meta.GameAsset `json:"GameMeta,omitempty"`
}

// JSONFormat implements atomic.JSONFormatter by wrapping json.Marshal
func (e Eventlog) JSONFormat() ([]byte, error) {
	data := e.EventLog.DynamicEventLog
	if data == nil {
		return json.Marshal(e)
	}
	data["GameMeta"] = e.GameMeta
	return json.Marshal(data)
}

// GetAsset is a getter for receiving event source and target information
// For exampe, event source for syslog is usually the shipper, while suricata alert has affected source and destination IP addresses whereas directionality matters
// Should provide needed information for doing external asset table lookups
func (e Eventlog) GetAsset() *meta.GameAsset {
	return &meta.GameAsset{
		Asset: meta.Asset{
			Host: e.Sender(),
			IP:   e.SenderIP(),
		},
	}
}

// SetAsset is a setter for setting meta to object without knowing the object type
// all asset lookups and field discoveries should be done before using this method to maintain readability
func (e *Eventlog) SetAsset(data meta.GameAsset) {
	e.GameMeta = data
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (e Eventlog) Time() time.Time { return e.EventLog.Time() }

// Source implements atomic.Event
// Source of message, usually emitting program
func (e Eventlog) Source() string { return e.EventLog.Source() }

// Sender implements atomic.Event
// Sender of message, usually a host
func (e Eventlog) Sender() string { return e.EventLog.Sender() }

type ZeekCobalt struct {
	atomic.ZeekCobalt
	GameMeta meta.GameAsset `json:"GameMeta,omitempty"`
}

// JSONFormat implements atomic.JSONFormatter by wrapping json.Marshal
func (z ZeekCobalt) JSONFormat() ([]byte, error) { return json.Marshal(z) }

// GetAsset is a getter for receiving event source and target information
// For exampe, event source for syslog is usually the shipper, while suricata alert has affected source and destination IP addresses whereas directionality matters
// Should provide needed information for doing external asset table lookups
func (z ZeekCobalt) GetAsset() *meta.GameAsset {
	ipFn := func(ip *fields.StringIP) net.IP {
		if ip != nil {
			return ip.IP
		}
		return nil
	}
	src := ipFn(z.IDOrigH)
	srcFn := func(ip net.IP) *meta.Asset {
		if ip != nil {
			return &meta.Asset{IP: ip}
		}
		return nil
	}
	return &meta.GameAsset{
		Asset:       meta.Asset{IP: src},
		Source:      srcFn(src),
		Destination: srcFn(ipFn(z.IDRespH)),
	}
}

// SetAsset is a setter for setting meta to object without knowing the object type
// all asset lookups and field discoveries should be done before using this method to maintain readability
func (z *ZeekCobalt) SetAsset(data meta.GameAsset) {
	z.GameMeta = data
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (z ZeekCobalt) Time() time.Time { return z.ZeekCobalt.Time() }

// Source implements atomic.Event
// Source of message, usually emitting program
func (z ZeekCobalt) Source() string { return z.ZeekCobalt.Source() }

// Sender implements atomic.Event
// Sender of message, usually a host
func (z ZeekCobalt) Sender() string { return z.ZeekCobalt.Sender() }

type MazeRunner struct {
	atomic.MazeRunner
	GameMeta meta.GameAsset `json:"GameMeta,omitempty"`
}

// JSONFormat implements atomic.JSONFormatter by wrapping json.Marshal
func (m MazeRunner) JSONFormat() ([]byte, error) { return json.Marshal(m) }

// GetAsset is a getter for receiving event source and target information
// For exampe, event source for syslog is usually the shipper, while suricata alert has affected source and destination IP addresses whereas directionality matters
// Should provide needed information for doing external asset table lookups
func (m MazeRunner) GetAsset() *meta.GameAsset {
	srcFn := func(ip net.IP) *meta.Asset {
		if ip != nil {
			return &meta.Asset{IP: ip}
		}
		return nil
	}
	return &meta.GameAsset{
		Asset:       meta.Asset{Host: m.MazeRunner.Sender()},
		Source:      srcFn(m.MazeRunner.GetSrcIP()),
		Destination: srcFn(m.MazeRunner.GetDstIP()),
	}
}

// SetAsset is a setter for setting meta to object without knowing the object type
// all asset lookups and field discoveries should be done before using this method to maintain readability
func (m *MazeRunner) SetAsset(data meta.GameAsset) {
	m.GameMeta = data
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (m MazeRunner) Time() time.Time { return m.MazeRunner.Time() }

// Source implements atomic.Event
// Source of message, usually emitting program
func (m MazeRunner) Source() string { return m.MazeRunner.Source() }

// Sender implements atomic.Event
// Sender of message, usually a host
func (m MazeRunner) Sender() string { return m.MazeRunner.Sender() }
