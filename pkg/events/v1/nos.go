package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ccdcoe/go-peek/internal/types"
)

type NOS struct {
	Timestamp time.Time `json:"@timestamp"`
	Src       *stringIP `json:"src"`
	Dst       *stringIP `json:"dst"`
	Fp        string    `json:"fp"`

	GameMeta *Source `json:"gamemeta,omitempty"`
}

func NewNOS(raw []byte) (*NOS, error) {
	var n = &NOS{}
	if err := json.Unmarshal(raw, n); err != nil {
		return nil, err
	}
	return n, nil
}

func (n NOS) Key() string {
	return ""
}

func (n *NOS) Source() (*Source, error) {
	if n.GameMeta == nil {
		n.setMeta()
	}
	return n.GameMeta, nil
}

func (n *NOS) Rename(pretty string) {
	if n.GameMeta != nil {
		n.GameMeta.Host = pretty
	}
}

func (n NOS) JSON() ([]byte, error) {
	return json.Marshal(n)
}

func (n *NOS) SaganString() (string, error) {
	return "NOT IMPLEMENTED", &types.ErrNotImplemented{
		Err: fmt.Errorf(
			"SaganString method not implemented for zeek cobalt detect event",
		),
	}
}

func (n NOS) GetEventTime() time.Time {
	return n.Timestamp
}

func (n NOS) GetSyslogTime() time.Time {
	return n.Timestamp
}

func (n *NOS) setMeta() *NOS {
	n.GameMeta = NewSource()
	if n.Src != nil {
		n.GameMeta.SetIp(n.Src.IP)
		n.GameMeta.SetSrcIp(n.Src.IP)
	} else {
		fmt.Println("nos src missing")
	}
	if n.Dst != nil {
		n.GameMeta.SetDestIp(n.Dst.IP)
	} else {
		fmt.Println("nos dest missing")
	}
	return n
}
