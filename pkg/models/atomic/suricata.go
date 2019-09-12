package atomic

import (
	"time"

	"github.com/ccdcoe/go-peek/pkg/models/fields"
)

type DynamicSuricataEve map[string]interface{}

type StaticSuricataEve struct {
	EveBase

	Alert *EveAlert `json:"alert,omitempty"`

	SSH  map[string]interface{} `json:"ssh,omitempty"`
	TLS  map[string]interface{} `json:"tls,omitempty"`
	TCP  map[string]interface{} `json:"tcp,omitempty"`
	DNS  map[string]interface{} `json:"dns,omitempty"`
	HTTP map[string]interface{} `json:"http,omitempty"`

	Dhcp       map[string]interface{} `json:"dhcp,omitempty"`
	PacketInfo map[string]interface{} `json:"packet_info,omitempty"`
	Smb        map[string]interface{} `json:"smb,omitempty"`
	Traffic    map[string]interface{} `json:"traffic,omitempty"`
	Fileinfo   map[string]interface{} `json:"fileinfo,omitempty"`
	Flow       map[string]interface{} `json:"flow,omitempty"`
	Tftp       map[string]interface{} `json:"tftp,omitempty"`
	Krb5       map[string]interface{} `json:"krb5,omitempty"`
	Snmp       map[string]interface{} `json:"snmp,omitempty"`
	Ikev2      map[string]interface{} `json:"ikev2,omitempty"`
	Tunnel     map[string]interface{} `json:"tunnel,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
	Anomaly    map[string]interface{} `json:"anomaly,omitempty"`
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (s StaticSuricataEve) Time() time.Time { return s.EveBase.Timestamp.Time }

// Source implements atomic.Event
// Source of message, usually emitting program
func (s StaticSuricataEve) Source() string { return s.EveBase.EventType }

// Sender implements atomic.Event
// Sender of message, usually a host
func (s StaticSuricataEve) Sender() string { return s.EveBase.Host }

type EveBase struct {
	Timestamp *fields.QuotedRFC3339 `json:"timestamp,omitempty"`
	Host      string                `json:"host,omitempty"`

	FlowID      int64  `json:"flow_id,omitempty"`
	InIface     string `json:"in_iface,omitempty"`
	EventType   string `json:"event_type,omitempty"`
	Proto       string `json:"proto,omitempty"`
	CommunityID string `json:"community_id,omitempty"`

	SrcIP    *fields.StringIP `json:"src_ip,omitempty"`
	SrcPort  int              `json:"src_port,omitempty"`
	DestIP   *fields.StringIP `json:"dest_ip,omitempty"`
	DestPort int              `json:"dest_port,omitempty"`

	AppProto         string `json:"app_proto,omitempty"`
	Payload          string `json:"payload,omitempty"`
	PayloadPrintable string `json:"payload_printable,omitempty"`
	Stream           int    `json:"stream,omitempty"`
	TXid             int    `json:"t_xid,omitempty"`
	Vlan             []int  `json:"vlan,omitempty"`

	NetInfo *NetInfo `json:"net_info,omitempty"`
}

type EveAlert struct {
	Action      string `json:"action,omitempty"`
	Gid         int    `json:"gid,omitempty"`
	SignatureID int    `json:"signature_id,omitempty"`
	Rev         int    `json:"rev,omitempty"`
	Signature   string `json:"signature,omitempty"`
	Category    string `json:"category,omitempty"`
	Severity    int    `json:"severity,omitempty"`

	Metadata map[string]interface{} `json:"metadata,omitempty"`

	Source *SrcTargetInfo `json:"source,omitempty"`
	Target *SrcTargetInfo `json:"target,omitempty"`
}

type NetInfo struct {
	Src  []string `json:"src,omitempty"`
	Dest []string `json:"dest,omitempty"`
}

type SrcTargetInfo struct {
	IP      *fields.StringIP `json:"ip,omitempty"`
	Port    int              `json:"port,omitempty"`
	NetInfo []string         `json:"net_info,omitempty"`
}
