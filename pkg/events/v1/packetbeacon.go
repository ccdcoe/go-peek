package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ccdcoe/go-peek/internal/types"
)

type PacketBeacon struct {
	Src        *stringIP `json:"src"`
	Dst        *stringIP `json:"dst"`
	P          string    `json:"p"`
	Proto      string    `json:"proto"`
	Note       string    `json:"note"`
	Msg        string    `json:"msg"`
	Likelihood string    `json:"likelihood"`
	Period     string    `json:"period"`
	Ts         time.Time `json:"ts"`

	GameMeta *Source `json:"gamemeta,omitempty"`
}

func NewPacketBeacon(raw []byte) (*PacketBeacon, error) {
	var p = &PacketBeacon{}
	if err := json.Unmarshal(raw, p); err != nil {
		return nil, err
	}
	return p, nil
}

func (p PacketBeacon) Key() string {
	return p.Note
}

func (p *PacketBeacon) Source() (*Source, error) {
	if p.GameMeta == nil {
		p.setMeta()
	}
	return p.GameMeta, nil
}

func (p *PacketBeacon) Rename(pretty string) {
	p.GameMeta.Host = pretty
}

func (p PacketBeacon) JSON() ([]byte, error) {
	return json.Marshal(p)
}

func (p *PacketBeacon) SaganString() (string, error) {
	return "NOT IMPLEMENTED", &types.ErrNotImplemented{
		Err: fmt.Errorf(
			"SaganString method not implemented for packet inverval detector",
		),
	}
}

func (p PacketBeacon) GetEventTime() time.Time {
	return p.Ts
}

func (p PacketBeacon) GetSyslogTime() time.Time {
	return p.Ts
}

func (p *PacketBeacon) setMeta() *PacketBeacon {
	p.GameMeta = NewSource()
	if p.Src != nil {
		p.GameMeta.SetIp(p.Src.IP)
		p.GameMeta.SetSrcIp(p.Src.IP)
	}
	if p.Dst != nil {
		p.GameMeta.SetDestIp(p.Dst.IP)
	}
	return p
}
