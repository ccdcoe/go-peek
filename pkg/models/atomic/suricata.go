package atomic

import (
	"time"

	"go-peek/pkg/models/fields"
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
	RDP  map[string]interface{} `json:"rdp,omitempty"`
	FTP  map[string]interface{} `json:"ftp,omitempty"`
	SMB  map[string]interface{} `json:"smb,omitempty"`
	DHCP map[string]interface{} `json:"dhcp,omitempty"`
	SNMP map[string]interface{} `json:"snmp,omitempty"`
	TFTP map[string]interface{} `json:"tftp,omitempty"`
	SIP  map[string]interface{} `json:"sip,omitempty"`

	FTPdata    map[string]interface{} `json:"ftp_data,omitempty"`
	PacketInfo map[string]interface{} `json:"packet_info,omitempty"`
	Traffic    map[string]interface{} `json:"traffic,omitempty"`
	Fileinfo   map[string]interface{} `json:"fileinfo,omitempty"`
	Flow       map[string]interface{} `json:"flow,omitempty"`
	Krb5       map[string]interface{} `json:"krb5,omitempty"`
	Ikev2      map[string]interface{} `json:"ikev2,omitempty"`
	Tunnel     map[string]interface{} `json:"tunnel,omitempty"`
	Anomaly    map[string]interface{} `json:"anomaly,omitempty"`

	Metadata map[string][]string `json:"metadata,omitempty"`
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (s StaticSuricataEve) Time() time.Time {
	if s.EveBase.Timestamp == nil {
		return time.Time{}
	}
	return s.EveBase.Timestamp.Time
}

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
	TXid             int    `json:"tx_id,omitempty"`
	Vlan             []int  `json:"vlan,omitempty"`

	NetInfo *NetInfo `json:"net_info,omitempty"`
}

func (e EveBase) GetField(key string) (interface{}, bool) {
	switch key {
	case "timestamp":
		return e.Timestamp.String(), true
	case "host":
		return e.Host, true
	case "event_type":
		return e.EventType, true
	case "proto":
		return e.Proto, true
	case "community_id":
		return e.CommunityID, true
	case "src_ip":
		if e.SrcIP != nil {
			return e.SrcIP.IP.String(), true
		}
	case "src_port":
		return e.DestPort, true
	case "dest_ip":
		if e.DestIP != nil {
			return e.DestIP.IP.String(), true
		}
	case "dest_port":
		return e.DestPort, true
	case "app_proto":
		return e.AppProto, true
	case "payload":
		return e.Payload, true
	case "payload_printable":
		return e.PayloadPrintable, true
	case "stream":
		return e.Stream, true
	case "tx_id":
		return e.TXid, true
	case "vlan":
		return e.Vlan, true
	}
	return nil, false
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

func (e EveAlert) GetField(key string) (interface{}, bool) {
	switch key {
	case "action":
		return e.Action, true
	case "gid":
		return e.Gid, true
	case "signature_id":
		return e.SignatureID, true
	case "category":
		return e.Category, true
	case "severity":
		return e.Severity, true
	case "metadata":
		return e.Metadata, true
	}
	return nil, false
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
