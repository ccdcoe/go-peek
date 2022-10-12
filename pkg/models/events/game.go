package events

import (
	"encoding/json"
	"fmt"
	"go-peek/pkg/models/atomic"
	"go-peek/pkg/models/meta"
	"net"
	"strings"
	"time"

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
	Data      atomic.DynamicSuricataEve
	Timestamp time.Time       `json:"@timestamp"`
	Syslog    *atomic.Syslog  `json:"syslog,omitempty"`
	GameMeta  *meta.GameAsset `json:"GameMeta,omitempty"`
}

func (s Suricata) Emit() bool {
	return s.GameMeta != nil && s.GameMeta.MitreAttack != nil
}

func (s Suricata) Kind() Atomic { return SuricataE }

func (s Suricata) GetMitreAttack() *meta.MitreAttack {
	metadata, ok := s.Data["metadata"].(map[string]any)
	if !ok {
		return &meta.MitreAttack{}
	}
	technique, ok := metadata["mitre_technique_id"].([]any)
	if !ok || len(technique) != 1 {
		return &meta.MitreAttack{}
	}
	val, ok := technique[0].(string)
	if !ok {
		return &meta.MitreAttack{}
	}

	return &meta.MitreAttack{Techniques: []meta.Technique{{ID: val}}}
}

// DumpEventData implements EventDataDumper
func (s Suricata) DumpEventData() *meta.EventData {
	evType, ok := s.Data["event_type"].(string)
	if !ok {
		return nil
	}
	meta := &meta.EventData{Key: evType, Fields: []string{}}
	if evType != "alert" {
		// Ignore other event types for now, too much variance and final dashboard only consumes alerts
		return meta
	}
	// extract signature_id
	rawSigID, ok := getField("alert.signature_id", s.Data)
	if !ok {
		return meta
	}
	// TODO - can generics help here?
	switch sigID := rawSigID.(type) {
	case float64:
		meta.ID = int(sigID)
	case int:
		meta.ID = sigID
	case int64:
		meta.ID = int(sigID)
	}
	rawSigName, ok := getField("alert.signature_id", s.Data)
	if ok {
		sigName, ok := rawSigName.(string)
		if ok {
			meta.Fields = append(meta.Fields, sigName)
		}
	}
	rawSigCat, ok := getField("alert.category", s.Data)
	if ok {
		sigCat, ok := rawSigCat.(string)
		if ok {
			meta.Fields = append(meta.Fields, sigCat)
		}
	}

	return meta
}

// Keywords implements Keyworder
func (s Suricata) Keywords() ([]string, bool) {
	tx := make([]string, 0)
	evType, ok := s.Data["event_type"].(string)
	if !ok {
		return tx, false
	}
	if evType == "alert" {
		rawSigName, ok := getField("alert.signature_id", s.Data)
		if ok {
			sigName, ok := rawSigName.(string)
			if ok {
				tx = append(tx, sigName)
			}
		}
		rawSigCat, ok := getField("alert.category", s.Data)
		if ok {
			sigCat, ok := rawSigCat.(string)
			if ok {
				tx = append(tx, sigCat)
			}
		}
	}
	pp, ok := s.Data["payload_printable"].(string)
	if ok {
		tx = append(tx, pp)
	}

	if len(tx) == 0 {
		return tx, false
	}
	return tx, true
}

// Select returns a success status and arbitrary field content if requested map key is present
func (s Suricata) Select(key string) (any, bool) { return getField(key, s.Data) }

// JSONFormat implements atomic.JSONFormatter by wrapping json.Marshal
func (s Suricata) JSONFormat() ([]byte, error) {
	s.Data["GameMeta"] = s.GameMeta
	return json.Marshal(s.Data)
}

// GetAsset is a getter for receiving event source and target information
// For exampe, event source for syslog is usually the shipper, while suricata alert has affected source and destination IP addresses whereas directionality matters
// Should provide needed information for doing external asset table lookups
func (s Suricata) GetAsset() *meta.GameAsset {
	asset := &meta.GameAsset{}
	if host, ok := s.Data["host"].(string); ok {
		asset.Asset = meta.Asset{Host: host}
	}
	if ipSrc, ok := s.Data["src_ip"].(string); ok {
		asset.Source = &meta.Asset{IP: net.ParseIP(ipSrc)}
	}
	if ipDest, ok := s.Data["dest_ip"].(string); ok {
		asset.Destination = &meta.Asset{IP: net.ParseIP(ipDest)}
	}
	return asset
}

// SetAsset is a setter for setting meta to object without knowing the object type
// all asset lookups and field discoveries should be done before using this method to maintain readability
func (s *Suricata) SetAsset(data *meta.GameAsset) {
	s.GameMeta = data
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (s Suricata) Time() time.Time { return s.Data.Time() }

// Source implements atomic.Event
// Source of message, usually emitting program
func (s Suricata) Source() string { return s.Data.Source() }

// Sender implements atomic.Event
// Sender of message, usually a host
func (s Suricata) Sender() string { return s.Data.Sender() }

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
