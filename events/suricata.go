package events

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

type suriTS struct{ time.Time }

func (t *suriTS) UnmarshalJSON(b []byte) error {
	raw, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	t.Time, err = time.Parse("2006-01-02T15:04:05.999999-0700", raw)
	return err
}

type Eve struct {
	Syslog
	EveBase

	SuriTime *suriTS      `json:"timestamp,omitempty"`
	Alert    *EveAlert    `json:"alert,omitempty"`
	DNS      *EveDNS      `json:"dns,omitempty"`
	TLS      *EveTLS      `json:"tls,omitempty"`
	SMB      *EveSmb      `json:"smb,omitempty"`
	Http     *EveHttp     `json:"http,omitempty"`
	Fileinfo *EveFileInfo `json:"fileinfo,omitempty"`

	GameMeta *Source `json:"gamemeta,omitempty"`
}

// JSON implements Event
func (s Eve) JSON() ([]byte, error) {
	return json.Marshal(s)
}

// Source implements Event
func (s *Eve) Source() *Source {
	s.GameMeta = &Source{
		Host: s.Syslog.Host,
		IP:   s.IP.String(),
		Src:  &Source{},
		Dest: &Source{},
	}
	switch v := s.EventType; {
	case v == "alert":
		if s.Alert.Source != nil {
			s.GameMeta.Src.IP = s.Alert.Source.IP
		} else {
			s.GameMeta.Src.IP = s.SrcIP.String()
		}
		if s.Alert.Target != nil {
			s.GameMeta.Dest.IP = s.Alert.Target.IP
		} else {
			s.GameMeta.Dest.IP = s.DestIP.String()
		}
	case v != "stats":
		s.GameMeta.Src.IP = s.SrcIP.String()
		s.GameMeta.Dest.IP = s.DestIP.String()
	}

	return s.GameMeta
}

// Rename implements Event
func (s *Eve) Rename(pretty string) {
	s.Host = pretty
}

func (s Eve) Key() string {
	return s.EventType
}

func (s Eve) GetEventTime() time.Time {
	return s.SuriTime.Time
}
func (s Eve) GetSyslogTime() time.Time {
	return s.Timestamp
}
func (s Eve) SaganString() (string, error) {
	return "NOT IMPLEMENTED", fmt.Errorf(
		"SaganString method not implemented for suricata eve.json",
	)
}

// Logical grouping of varous EVE.json components
type EveBase struct {
	FlowID      int64     `json:"flow_id,omitempty"`
	InIface     string    `json:"in_iface,omitempty"`
	EventType   string    `json:"event_type,omitempty"`
	SrcIP       *stringIP `json:"src_ip,omitempty"`
	SrcPort     int       `json:"src_port,omitempty"`
	DestIP      *stringIP `json:"dest_ip,omitempty"`
	DestPort    int       `json:"dest_port,omitempty"`
	Proto       string    `json:"proto,omitempty"`
	CommunityID string    `json:"community_id,omitempty"`
	SuriHost    string    `json:"host,omitempty"`
	NetInfo     *NetInfo  `json:"net_info,omitempty"`
}

type NetInfo struct {
	Src  []string `json:"src,omitempty"`
	Dest []string `json:"dest,omitempty"`
}

type SrcTargetInfo struct {
	IP      string   `json:"ip,omitempty"`
	Port    int      `json:"port,omitempty"`
	NetInfo []string `json:"net_info,omitempty"`
}

type EveAlert struct {
	Action      string         `json:"action,omitempty"`
	Gid         int            `json:"gid,omitempty"`
	SignatureID int            `json:"signature_id,omitempty"`
	Rev         int            `json:"rev,omitempty"`
	Signature   string         `json:"signature,omitempty"`
	Category    string         `json:"category,omitempty"`
	Severity    int            `json:"severity,omitempty"`
	Metadata    *EveMetadata   `json:"metadata,omitempty"`
	Source      *SrcTargetInfo `json:"source,omitempty"`
	Target      *SrcTargetInfo `json:"target,omitempty"`
}

type EveFlow struct {
	PktsToserver  int    `json:"pkts_toserver,omitempty"`
	PktsToclient  int    `json:"pkts_toclient,omitempty"`
	BytesToserver int    `json:"bytes_toserver,omitempty"`
	BytesToclient int    `json:"bytes_toclient,omitempty"`
	Start         string `json:"start,omitempty"`
}

type EveMetadata struct {
	UpdatedAt      []string `json:"updated_at,omitempty"`
	CreatedAt      []string `json:"created_at,omitempty"`
	FormerCategory []string `json:"former_category,omitempty"`
}

type EveDNS struct {
	Version int    `json:"version,omitempty"`
	Type    string `json:"type,omitempty"`
	ID      int    `json:"id,omitempty"`
	Flags   string `json:"flags,omitempty"`
	Qr      bool   `json:"qr,omitempty"`
	Rd      bool   `json:"rd,omitempty"`
	Ra      bool   `json:"ra,omitempty"`
	Rcode   string `json:"rcode,omitempty"`

	*EveDNSanswer

	//Grouped map[string]EveDNSanswer `json:"grouped,omitempty"`
	Answers []EveDNSanswer `json:"answers,omitempty"`
}

type EveDNSanswer struct {
	Rrname string `json:"rrname,omitempty"`
	Rrtype string `json:"rrtype,omitempty"`
	TTL    int    `json:"ttl,omitempty"`
	Rdata  string `json:"rdata,omitempty"`
}

type EveSmb struct {
	ID         int    `json:"id,omitempty"`
	Dialect    string `json:"dialect,omitempty"`
	Command    string `json:"command,omitempty"`
	Status     string `json:"status,omitempty"`
	StatusCode string `json:"status_code,omitempty"`
	SessionID  int64  `json:"session_id,omitempty"`
	TreeID     int    `json:"tree_id,omitempty"`
	Dcerpc     struct {
		Request    string `json:"request,omitempty"`
		Response   string `json:"response,omitempty"`
		CallID     int    `json:"call_id,omitempty"`
		Interfaces []struct {
			UUID      string `json:"uuid,omitempty"`
			Version   string `json:"version,omitempty"`
			AckResult int    `json:"ack_result,omitempty"`
			AckReason int    `json:"ack_reason,omitempty"`
		} `json:"interfaces,omitempty"`
	} `json:"dcerpc,omitempty"`
}

type EveSmbInterfaces struct {
	UUID      string `json:"uuid,omitempty"`
	Version   string `json:"version,omitempty"`
	AckResult int    `json:"ack_result,omitempty"`
	AckReason int    `json:"ack_reason,omitempty"`
}

type EveHttp struct {
	Hostname        string `json:"hostname,omitempty"`
	URL             string `json:"url,omitempty"`
	HTTPUserAgent   string `json:"http_user_agent,omitempty"`
	HTTPContentType string `json:"http_content_type,omitempty"`
	HTTPRefer       string `json:"http_refer,omitempty"`
	HTTPMethod      string `json:"http_method,omitempty"`
	Protocol        string `json:"protocol,omitempty"`
	Status          int    `json:"status,omitempty"`
	Length          int    `json:"length,omitempty"`
}

type EveTLS struct {
	Subject     string `json:"subject,omitempty"`
	Issuerdn    string `json:"issuerdn,omitempty"`
	Serial      string `json:"serial,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	Sni         string `json:"sni,omitempty"`
	Version     string `json:"version,omitempty"`
	Notbefore   string `json:"notbefore,omitempty"`
	Notafter    string `json:"notafter,omitempty"`
}

type EveTftp struct {
	Packet string `json:"packet,omitempty"`
	File   string `json:"file,omitempty"`
	Mode   string `json:"mode,omitempty"`
}

type EveFileInfo struct {
	Filename string `json:"filename,omitempty"`
	Gaps     bool   `json:"gaps,omitempty"`
	State    string `json:"state,omitempty"`
	Stored   bool   `json:"stored,omitempty"`
	Size     int    `json:"size,omitempty"`
	TxID     int    `json:"tx_id,omitempty"`
}
