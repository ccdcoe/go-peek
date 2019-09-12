package atomic

import (
	"time"

	"github.com/ccdcoe/go-peek/pkg/models/fields"
)

// ZeekCobalt is a custom event from zeeky
// For detecting cobalt strike beacons
// https://github.com/ccdcoe/zeeky
type ZeekCobalt struct {
	// TODO - decode Ts directly to time.Time
	Timestamp   float64          `json:"ts"`
	UID         string           `json:"uid"`
	IDOrigH     *fields.StringIP `json:"id.orig_h,omitempty"`
	IDOrigP     int              `json:"id.orig_p"`
	IDRespH     *fields.StringIP `json:"id.resp_h,omitempty"`
	IDRespP     int              `json:"id.resp_p"`
	Proto       string           `json:"proto"`
	Note        string           `json:"note"`
	Msg         string           `json:"msg"`
	Sub         string           `json:"sub"`
	Src         *fields.StringIP `json:"src,omitempty"`
	Dst         *fields.StringIP `json:"dst,omitempty"`
	P           int              `json:"p"`
	Actions     []string         `json:"actions"`
	SuppressFor float64          `json:"suppress_for"`
	Dropped     bool             `json:"dropped"`
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (z ZeekCobalt) Time() time.Time { return time.Unix(int64(z.Timestamp), 0) }

// Source implements atomic.Event
// Source of message, usually emitting program
func (z ZeekCobalt) Source() string { return z.IDOrigH.String() }

// Sender implements atomic.Event
// Sender of message, usually a host
func (z ZeekCobalt) Sender() string { return "cobalt" }
