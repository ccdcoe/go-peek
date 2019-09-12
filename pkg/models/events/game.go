package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ccdcoe/go-peek/pkg/models/atomic"
	"github.com/ccdcoe/go-peek/pkg/models/meta"
)

type GameEvent interface {
	atomic.Event
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

func NewGameEvent(data []byte, enum Atomic) (GameEvent, error) {
	switch enum {
	case SuricataE:
		var obj Suricata
		if err := json.Unmarshal(data, &obj); err != nil {
			return nil, err
		}
		return obj, nil
	case SyslogE:
		var obj Syslog
		if err := json.Unmarshal(data, &obj); err != nil {
			return nil, err
		}
		return obj, nil
	case SnoopyE:
		var obj Snoopy
		if err := json.Unmarshal(data, &obj); err != nil {
			return nil, err
		}
		return obj, nil
	case EventLogE, SysmonE:
		var obj atomic.DynamicEventLog
		if err := json.Unmarshal(data, &obj); err != nil {
			return nil, err
		}
		static := &atomic.EventLog{
			DynamicEventLog: obj,
		}
		return &Eventlog{
			EventLog: *static.Parse(),
		}, nil
	case ZeekE:
		var obj ZeekCobalt
		if err := json.Unmarshal(data, &obj); err != nil {
			return nil, err
		}
		return obj, nil
	case MazeRunnerE:
		var obj MazeRunner
		if err := json.Unmarshal(data, &obj); err != nil {
			return nil, err
		}
		return obj, nil
	default:
		return nil, ErrEventParse{
			Data:   data,
			Wanted: enum,
			Reason: "object not supported",
		}
	}
}

type Suricata struct {
	atomic.StaticSuricataEve
	atomic.Syslog
	meta.Asset
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
	meta.Asset
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
	meta.Asset
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

type Eventlog struct {
	atomic.EventLog
	meta.Asset
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (e Eventlog) Time() time.Time { return e.EventLog.Time() }

// Source implements atomic.Event
// Source of message, usually emitting program
func (e Eventlog) Source() string { return e.EventLog.Source() }

// Sender implements atomic.Event
// Sender of message, usually a host
func (e Eventlog) Sender() string { return e.EventLog.Sender() }

type ZeekCobalt struct {
	atomic.ZeekCobalt
	meta.Asset
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (z ZeekCobalt) Time() time.Time { return z.ZeekCobalt.Time() }

// Source implements atomic.Event
// Source of message, usually emitting program
func (z ZeekCobalt) Source() string { return z.ZeekCobalt.Source() }

// Sender implements atomic.Event
// Sender of message, usually a host
func (z ZeekCobalt) Sender() string { return z.ZeekCobalt.Sender() }

type MazeRunner struct {
	atomic.MazeRunner
	meta.Asset
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (m MazeRunner) Time() time.Time { return m.MazeRunner.Time() }

// Source implements atomic.Event
// Source of message, usually emitting program
func (m MazeRunner) Source() string { return m.MazeRunner.Source() }

// Sender implements atomic.Event
// Sender of message, usually a host
func (m MazeRunner) Sender() string { return m.MazeRunner.Sender() }
