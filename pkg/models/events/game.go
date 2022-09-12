package events

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"go-peek/pkg/models/atomic"
	"go-peek/pkg/models/meta"

	"github.com/markuskont/go-sigma-rule-engine"
)

type Emitter interface {
	Emit() bool
}

type GameEvent interface {
	atomic.Event
	meta.AssetGetterSetter
	atomic.JSONFormatter
	meta.EventDataDumper
	meta.MitreGetter
	Kinder
	sigma.Event
	Emitter
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
	GameMeta *meta.GameAsset `json:"GameMeta,omitempty"`
}

func (d DynamicWinlogbeat) Emit() bool {
	return d.GameMeta != nil && d.GameMeta.MitreAttack != nil
}

func (d DynamicWinlogbeat) Kind() Atomic { return EventLogE }

func (d DynamicWinlogbeat) GetMitreAttack() *meta.MitreAttack {
	return d.MitreAttack()
}

// DumpEventData implements EventDataDumper
func (d DynamicWinlogbeat) DumpEventData() *meta.EventData {
	return &meta.EventData{
		Key: d.Source(),
		ID: func() int {
			if val, ok := getField("winlog.event_id", d.DynamicWinlogbeat); ok {
				num, ok := val.(float64)
				if ok {
					return int(num)
				}
			}
			return 0
		}(),
		Fields: func() []string {
			out := make([]string, 0)
			if val, ok := getField("process.name", d.DynamicWinlogbeat); ok {
				s, ok := val.(string)
				if ok {
					out = append(out, s)
				}
			}
			if val, ok := getField("winlog.event_data.TargetImage", d.DynamicWinlogbeat); ok {
				s, ok := val.(string)
				if ok {
					out = append(out, s)
				}
			}
			if val, ok := getField("winlog.task", d.DynamicWinlogbeat); ok {
				s, ok := val.(string)
				if ok {
					out = append(out, s)
				}
			}
			if val, ok := getField("winlog.user.name", d.DynamicWinlogbeat); ok {
				s, ok := val.(string)
				if ok {
					out = append(out, s)
				}
			}
			return out
		}(),
	}
}

func (d DynamicWinlogbeat) MitreAttack() *meta.MitreAttack {
	if val, ok := d.Select("rule.name"); ok {
		if s, ok := val.(string); ok {
			if bits := strings.Split(s, ","); len(bits) == 2 {
				technique := &meta.Technique{}
				if id := strings.Split(bits[0], "="); len(id) == 2 {
					technique.ID = id[1]
					if !strings.HasPrefix(strings.ToLower(technique.ID), "t") {
						technique.ID = "T" + technique.ID
					}
				}
				if name := strings.Split(bits[1], "="); len(name) == 2 {
					technique.Name = name[1]
				}
				return &meta.MitreAttack{
					Techniques: []meta.Technique{*technique},
				}
			}
		}
	}
	return nil
}

// Keywords implements Keyworder
func (d DynamicWinlogbeat) Keywords() ([]string, bool) {
	m, ok := d.DynamicWinlogbeat["message"].(string)
	if ok {
		return []string{m}, true
	}
	return nil, false
}

// Select returns a success status and arbitrary field content if requested map key is present
func (d DynamicWinlogbeat) Select(key string) (interface{}, bool) {
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
func (d *DynamicWinlogbeat) SetAsset(obj *meta.GameAsset) {
	d.GameMeta = obj
}

// JSONFormat implements atomic.JSONFormatter by wrapping json.Marshal
func (d DynamicWinlogbeat) JSONFormat() ([]byte, error) {
	obj := d.DynamicWinlogbeat
	if d.GameMeta != nil {
		obj["GameMeta"] = d.GameMeta
	}
	return json.Marshal(obj)
}

type Suricata struct {
	atomic.StaticSuricataEve
	Timestamp time.Time       `json:"@timestamp"`
	Syslog    *atomic.Syslog  `json:"syslog,omitempty"`
	GameMeta  *meta.GameAsset `json:"GameMeta,omitempty"`
}

func (s Suricata) Emit() bool {
	return s.GameMeta != nil && s.GameMeta.MitreAttack != nil
}

func (s Suricata) Kind() Atomic { return SuricataE }

func (s Suricata) GetMitreAttack() *meta.MitreAttack {
	if s.Metadata == nil || len(s.Metadata) == 0 {
		return nil
	}
	if val, ok := s.Metadata["mitre_technique_id"]; ok {
		if len(val) != 1 {
			return nil
		}
		return &meta.MitreAttack{Techniques: []meta.Technique{{ID: val[0]}}}
	}
	return nil
}

// DumpEventData implements EventDataDumper
func (s Suricata) DumpEventData() *meta.EventData {
	return &meta.EventData{
		ID: func() int {
			if s.Alert != nil {
				return s.Alert.SignatureID
			}
			return 0
		}(),
		Key: s.EventType,
		Fields: func() []string {
			if s.Alert == nil {
				return nil
			}
			return []string{
				s.Alert.Signature,
				s.Alert.Category,
			}
		}(),
	}
}

// Keywords implements Keyworder
func (s Suricata) Keywords() ([]string, bool) {
	out := []string{s.PayloadPrintable}
	if s.Alert != nil {
		out = append(out, []string{s.Alert.Signature, s.Alert.Category}...)
	}
	return out, true
}

// Select returns a success status and arbitrary field content if requested map key is present
func (s Suricata) Select(key string) (interface{}, bool) {
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
func (s *Suricata) SetAsset(data *meta.GameAsset) {
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
	atomic.Syslog
	GameMeta *meta.GameAsset `json:"GameMeta,omitempty"`
}

func (s Syslog) Emit() bool {
	return s.GameMeta != nil && s.GameMeta.MitreAttack != nil
}

func (s Syslog) Kind() Atomic { return SyslogE }

func (s Syslog) GetMitreAttack() *meta.MitreAttack {
	if s.GameMeta != nil {
		return s.GameMeta.MitreAttack
	}
	return nil
}

// DumpEventData implements EventDataDumper
func (s Syslog) DumpEventData() *meta.EventData {
	return &meta.EventData{
		ID:     0,
		Key:    s.Syslog.Program,
		Fields: []string{s.Syslog.Message},
	}
}

// Keywords implements Keyworder
func (s Syslog) Keywords() ([]string, bool) {
	return []string{s.Syslog.Message}, true
}

// Select returns a success status and arbitrary field content if requested map key is present
func (s Syslog) Select(key string) (interface{}, bool) {
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
func (s *Syslog) SetAsset(data *meta.GameAsset) {
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
	atomic.Syslog
	GameMeta *meta.GameAsset `json:"GameMeta,omitempty"`
}

func (s Snoopy) Emit() bool {
	return s.GameMeta != nil && s.GameMeta.MitreAttack != nil
}

func (s Snoopy) Kind() Atomic { return SnoopyE }

func (s Snoopy) GetMitreAttack() *meta.MitreAttack {
	if s.GameMeta != nil {
		return s.GameMeta.MitreAttack
	}
	return nil
}

// DumpEventData implements EventDataDumper
func (s Snoopy) DumpEventData() *meta.EventData {
	return &meta.EventData{
		ID:  0,
		Key: s.Filename,
		Fields: []string{
			s.Cmd,
			s.Cwd,
			s.Username,
		},
	}
}

// Keywords implements Keyworder
func (s Snoopy) Keywords() ([]string, bool) {
	return []string{s.Cmd}, true
}

// Select returns a success status and arbitrary field content if requested map key is present
func (s Snoopy) Select(key string) (interface{}, bool) {
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
func (s *Snoopy) SetAsset(data *meta.GameAsset) {
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
