package events

import (
	"encoding/json"
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
	SuriTime *suriTS     `json:"timestamp"`
	Alert    EveAlert    `json:"alert,omitempty"`
	DNS      EveDNS      `json:"dns,omitempty"`
	TLS      EveTLS      `json:"tls,omitempty"`
	SMB      EveSmb      `json:"smb,omitempty"`
	Http     EveHttp     `json:"http,omitempty"`
	Fileinfo EveFileInfo `json:"fileinfo,omitempty"`
}

// JSON implements Event
func (s Eve) JSON() ([]byte, error) {
	return json.Marshal(s)
}

// Source implements Event
func (s Eve) Source() Source {
	return Source{
		Host: s.Host,
		IP:   s.IP,
	}
}

// Rename implements Event
func (s *Eve) Rename(pretty string) {
	s.Host = pretty
}

// Logical grouping of varous EVE.json components

type EveBase struct {
	FlowID      int64  `json:"flow_id"`
	InIface     string `json:"in_iface"`
	EventType   string `json:"event_type"`
	SrcIP       string `json:"src_ip"`
	SrcPort     int    `json:"src_port"`
	DestIP      string `json:"dest_ip"`
	DestPort    int    `json:"dest_port"`
	Proto       string `json:"proto"`
	CommunityID string `json:"community_id"`
}

type EveAlert struct {
	Action      string      `json:"action"`
	Gid         int         `json:"gid"`
	SignatureID int         `json:"signature_id"`
	Rev         int         `json:"rev"`
	Signature   string      `json:"signature"`
	Category    string      `json:"category"`
	Severity    int         `json:"severity"`
	Metadata    EveMetadata `json:"metadata,omitempty"`
}

type EveFlow struct {
	PktsToserver  int    `json:"pkts_toserver"`
	PktsToclient  int    `json:"pkts_toclient"`
	BytesToserver int    `json:"bytes_toserver"`
	BytesToclient int    `json:"bytes_toclient"`
	Start         string `json:"start"`
}

type EveMetadata struct {
	UpdatedAt      []string `json:"updated_at"`
	CreatedAt      []string `json:"created_at"`
	FormerCategory []string `json:"former_category"`
}

type EveDNS struct {
	Version int    `json:"version"`
	Type    string `json:"type"`
	ID      int    `json:"id"`
	Flags   string `json:"flags"`
	Qr      bool   `json:"qr"`
	Rd      bool   `json:"rd"`
	Ra      bool   `json:"ra"`
	Rcode   string `json:"rcode"`

	EveDNSanswer

	//Grouped map[string]EveDNSanswer `json:"grouped,omitempty"`
	Answers []EveDNSanswer `json:"answers,omitempty"`
}

type EveDNSanswer struct {
	Rrname string `json:"rrname"`
	Rrtype string `json:"rrtype"`
	TTL    int    `json:"ttl"`
	Rdata  string `json:"rdata"`
}

type EveSmb struct {
	ID         int    `json:"id"`
	Dialect    string `json:"dialect"`
	Command    string `json:"command"`
	Status     string `json:"status"`
	StatusCode string `json:"status_code"`
	SessionID  int64  `json:"session_id"`
	TreeID     int    `json:"tree_id"`
	Dcerpc     struct {
		Request    string `json:"request"`
		Response   string `json:"response"`
		Interfaces []struct {
			UUID      string `json:"uuid"`
			Version   string `json:"version"`
			AckResult int    `json:"ack_result"`
			AckReason int    `json:"ack_reason"`
		} `json:"interfaces"`
		CallID int `json:"call_id"`
	} `json:"dcerpc"`
}

type EveHttp struct {
	Hostname        string `json:"hostname"`
	URL             string `json:"url"`
	HTTPUserAgent   string `json:"http_user_agent"`
	HTTPContentType string `json:"http_content_type"`
	HTTPRefer       string `json:"http_refer"`
	HTTPMethod      string `json:"http_method"`
	Protocol        string `json:"protocol"`
	Status          int    `json:"status"`
	Length          int    `json:"length"`
}

type EveTLS struct {
	Subject     string `json:"subject"`
	Issuerdn    string `json:"issuerdn"`
	Serial      string `json:"serial"`
	Fingerprint string `json:"fingerprint"`
	Sni         string `json:"sni"`
	Version     string `json:"version"`
	Notbefore   string `json:"notbefore"`
	Notafter    string `json:"notafter"`
}

type EveTftp struct {
	Packet string `json:"packet"`
	File   string `json:"file"`
	Mode   string `json:"mode"`
}

type EveFileInfo struct {
	Filename string `json:"filename"`
	Gaps     bool   `json:"gaps"`
	State    string `json:"state"`
	Stored   bool   `json:"stored"`
	Size     int    `json:"size"`
	TxID     int    `json:"tx_id"`
}
