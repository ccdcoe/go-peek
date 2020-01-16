package events

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ccdcoe/go-peek/pkg/models/atomic"
	"github.com/ccdcoe/go-peek/pkg/models/fields"
	"github.com/ccdcoe/go-peek/pkg/models/meta"
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

type DynamicWinlogbeat struct {
	Timestamp time.Time `json:"@timestamp"`
	atomic.DynamicWinlogbeat
	GameMeta meta.GameAsset `json:"GameMeta,omitempty"`
}

// GetMessage implements MessageGetter
func (d DynamicWinlogbeat) GetMessage() []string {
	m, ok := d.DynamicWinlogbeat["message"].(string)
	if ok {
		return []string{m}
	}
	return nil
}

// GetField returns a success status and arbitrary field content if requested map key is present
func (d DynamicWinlogbeat) GetField(key string) (interface{}, bool) {
	return getField(key, d.DynamicWinlogbeat)
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
		MitreAttack: &meta.MitreAttack{
			Techniques: make([]meta.Technique, 0),
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
	obj := d.DynamicWinlogbeat
	obj["GameMeta"] = d.GameMeta
	return json.Marshal(obj)
}

type Suricata struct {
	atomic.StaticSuricataEve
	Syslog   *atomic.Syslog `json:"syslog,omitempty"`
	GameMeta meta.GameAsset `json:"GameMeta,omitempty"`
}

// GetMessage implements MessageGetter
func (s Suricata) GetMessage() []string {
	out := []string{s.PayloadPrintable}
	if s.Alert != nil {
		out = append(out, []string{s.Alert.Signature, s.Alert.Category}...)
	}
	return out
}

// GetField returns a success status and arbitrary field content if requested map key is present
func (s Suricata) GetField(key string) (interface{}, bool) {
	if !strings.Contains(key, ".") {
		return s.StaticSuricataEve.EveBase.GetField(key)
	}
	bits := strings.SplitN(key, ".", 1)
	if len(bits) == 2 && strings.HasPrefix(bits[0], "alert") && s.Alert != nil {
		return s.Alert.GetField(bits[1])
	}
	switch bits[0] {
	case "alert":
		if s.Alert != nil {
			return s.Alert.GetField(bits[1])
		}
	case "ssh":
		return getField(bits[1], s.SSH)
	case "tls":
		return getField(bits[1], s.TLS)
	case "tcp":
		return getField(bits[1], s.TCP)
	case "dns":
		return getField(bits[1], s.DNS)
	case "http":
		return getField(bits[1], s.HTTP)
	case "rdp":
		return getField(bits[1], s.RDP)
	case "smb":
		return getField(bits[1], s.SMB)
	case "dhcp":
		return getField(bits[1], s.DHCP)
	case "snmp":
		return getField(bits[1], s.SNMP)
	case "tftp":
		return getField(bits[1], s.TFTP)
	case "sip":
		return getField(bits[1], s.SIP)
	case "ftp_data":
		return getField(bits[1], s.FTPdata)
	case "packet_info":
		return getField(bits[1], s.PacketInfo)
	case "traffic":
		return getField(bits[1], s.Traffic)
	case "fileinfo":
		return getField(bits[1], s.Fileinfo)
	case "flow":
		return getField(bits[1], s.Flow)
	case "krb5":
		return getField(bits[1], s.Krb5)
	case "ikev2":
		return getField(bits[1], s.Ikev2)
	case "tunnel":
		return getField(bits[1], s.Tunnel)
	case "metadata":
		return getField(bits[1], s.Metadata)
	case "anomaly":
		return getField(bits[1], s.Anomaly)
	}
	return nil, false
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
		MitreAttack: &meta.MitreAttack{
			Techniques: make([]meta.Technique, 0),
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

// GetMessage implements MessageGetter
func (s Syslog) GetMessage() []string {
	return []string{s.Syslog.Message}
}

// GetField returns a success status and arbitrary field content if requested map key is present
func (s Syslog) GetField(key string) (interface{}, bool) {
	return s.Syslog.GetField(key)
}

// JSONFormat implements atomic.JSONFormatter by wrapping json.Marshal
func (s Syslog) JSONFormat() ([]byte, error) { return json.Marshal(s) }

// GetAsset is a getter for receiving event source and target information
// For exampe, event source for syslog is usually the shipper, while suricata alert has affected source and destination IP addresses whereas directionality matters
// Should provide needed information for doing external asset table lookups
func (s Syslog) GetAsset() *meta.GameAsset {
	return &meta.GameAsset{
		Directionality: meta.DirLocal,
		MitreAttack: &meta.MitreAttack{
			Techniques: make([]meta.Technique, 0),
		},
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

// GetMessage implements MessageGetter
func (s Snoopy) GetMessage() []string {
	return []string{s.Cmd}
}

// GetField returns a success status and arbitrary field content if requested map key is present
func (s Snoopy) GetField(key string) (interface{}, bool) {
	switch key {
	case "cmd":
		return s.Cmd, true
	case "filename":
		return s.Filename, true
	case "cwd":
		return s.Cwd, true
	case "tty":
		return s.Tty, true
	case "sid":
		return s.Sid, true
	case "gid":
		return s.Gid, true
	case "group":
		return s.Group, true
	case "uid":
		return s.UID, true
	case "username":
		return s.Username, true
	case "login":
		return s.Login, true
	}
	if s.SSH != nil && strings.HasPrefix("ssh", key) {
		bits := strings.SplitN(key, ".", 1)
		switch bits[1] {
		case "dst_port", "dest_port", "dport":
			return s.SSH.DstPort, true
		case "dst_ip", "dest_ip":
			if s.SSH.DstIP != nil {
				return s.SSH.DstIP.IP.String(), true
			}
		case "src_port", "sport":
			return s.SSH.SrcPort, true
		case "src_ip":
			if s.SSH.SrcIP != nil {
				return s.SSH.SrcIP.IP.String(), true
			}
		}
	}
	return s.Syslog.GetField(key)
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
		MitreAttack: &meta.MitreAttack{
			Techniques: make([]meta.Technique, 0),
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

// GetMessage implements MessageGetter
func (e *Eventlog) GetMessage() []string {
	panic("not implemented") // TODO: Implement
}

// GetField returns a success status and arbitrary field content if requested map key is present
func (e *Eventlog) GetField(_ string) (interface{}, bool) {
	panic("not implemented") // TODO: Implement
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
		MitreAttack: &meta.MitreAttack{
			Techniques: make([]meta.Technique, 0),
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

// GetMessage implements MessageGetter
func (z *ZeekCobalt) GetMessage() []string {
	panic("not implemented") // TODO: Implement
}

// GetField returns a success status and arbitrary field content if requested map key is present
func (z *ZeekCobalt) GetField(_ string) (interface{}, bool) {
	panic("not implemented") // TODO: Implement
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
		Asset: meta.Asset{IP: src},
		MitreAttack: &meta.MitreAttack{
			Techniques: make([]meta.Technique, 0),
		},
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

// GetMessage implements MessageGetter
func (m *MazeRunner) GetMessage() []string {
	panic("not implemented") // TODO: Implement
}

// GetField returns a success status and arbitrary field content if requested map key is present
func (m *MazeRunner) GetField(_ string) (interface{}, bool) {
	panic("not implemented") // TODO: Implement
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
		Asset: meta.Asset{Host: m.MazeRunner.Sender()},
		MitreAttack: &meta.MitreAttack{
			Techniques: make([]meta.Technique, 0),
		},
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
