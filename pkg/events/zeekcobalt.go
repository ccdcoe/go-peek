package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ccdcoe/go-peek/internal/types"
)

type ZeekCobalt struct {
	Timestamp   time.Time `json:"@timestamp"`
	Ts          float64   `json:"ts"`
	UID         string    `json:"uid"`
	IDOrigH     *stringIP `json:"id.orig_h"`
	IDOrigP     int       `json:"id.orig_p"`
	IDRespH     *stringIP `json:"id.resp_h"`
	IDRespP     int       `json:"id.resp_p"`
	Proto       string    `json:"proto"`
	Note        string    `json:"note"`
	Msg         string    `json:"msg"`
	Sub         string    `json:"sub"`
	Src         *stringIP `json:"src"`
	Dst         *stringIP `json:"dst"`
	P           int       `json:"p"`
	Actions     []string  `json:"actions"`
	SuppressFor float64   `json:"suppress_for"`
	Dropped     bool      `json:"dropped"`

	GameMeta *Source `json:"gamemeta,omitempty"`
}

func NewZeekCobalt(raw []byte) (*ZeekCobalt, error) {
	var z = &ZeekCobalt{}
	if err := json.Unmarshal(raw, z); err != nil {
		return nil, err
	}
	z.Timestamp = z.GetEventTime()
	return z, nil
}

func (z ZeekCobalt) Key() string {
	return z.Note
}

func (z *ZeekCobalt) Source() (*Source, error) {
	if z.GameMeta == nil {
		z.setMeta()
	}
	return z.GameMeta, nil
}

func (z *ZeekCobalt) Rename(pretty string) {
	if z.GameMeta != nil {
		z.GameMeta.Host = pretty
	}
}

func (z ZeekCobalt) JSON() ([]byte, error) {
	return json.Marshal(z)
}

func (z *ZeekCobalt) SaganString() (string, error) {
	return "NOT IMPLEMENTED", &types.ErrNotImplemented{
		Err: fmt.Errorf(
			"SaganString method not implemented for zeek cobalt detect event",
		),
	}
}

func (z *ZeekCobalt) GetEventTime() time.Time {
	return time.Unix(int64(z.Ts), 0)
}

func (z *ZeekCobalt) GetSyslogTime() time.Time {
	return time.Unix(int64(z.Ts), 0)
}

func (z *ZeekCobalt) setMeta() *ZeekCobalt {
	z.GameMeta = NewSource()
	if z.IDOrigH != nil {
		z.GameMeta.SetIp(z.IDOrigH.IP)
		z.GameMeta.SetSrcIp(z.IDOrigH.IP)
	}
	if z.IDRespH != nil {
		z.GameMeta.SetDestIp(z.IDRespH.IP)
	}
	return z
}
