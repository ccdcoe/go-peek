package events

import (
	"bytes"
	"strings"
	"time"
)

type Timer interface {
	Time() time.Time
}

type Anonymizer interface {
	Anonymize(map[string]string) error
}

type SaganFormatter interface {
	SaganFormat() string
}

type Kinder interface {
	Kind() Atomic
}

func NewAtomic(raw string) (Atomic, bool) {
	for _, atomic := range Atomics {
		if raw == atomic.String() {
			return atomic, true
		}
	}
	return SimpleE, false
}

type Atomic int

func (a Atomic) String() string {
	switch a {
	case KnownTimeStampsE:
		return "timestamp parser"
	case SuricataE:
		return "suricata"
	case SyslogE:
		return "syslog"
	case SnoopyE:
		return "snoopy"
	case EventLogE:
		return "windows"
	case SysmonE:
		return "sysmon"
	case ZeekE:
		return "zeek"
	case MazeRunnerE:
		return "mazerunner"
	default:
		return "atomic"
	}
}

func (a Atomic) Explain() string {
	switch a {
	case KnownTimeStampsE:
		return "Simple timestamp parser. " +
			"Attemtps to access popular timestamp fields in JSON messages"
	case SuricataE:
		return "Suricata EVE JSON format."
	case SyslogE:
		return "Parsed BSD syslog."
	case SnoopyE:
		return "Snoopy audit log for linux systems."
	case EventLogE:
		return "Windows event log."
	case SysmonE:
		return "Sysmon audit log for Windows event log."
	case ZeekE:
		return "Zeek, formerly known as Bro. Custom output format."
	case MazeRunnerE:
		return "MazeRunner. Honeypot system from Cymmertria."
	default:
		return "Simple fallback format for unknown JSON formats. " +
			"May attempt to access and parse popular timestamp keys but no guarantee on success."
	}
}

const (
	SimpleE Atomic = iota
	KnownTimeStampsE
	SuricataE
	SyslogE
	SnoopyE
	EventLogE
	SysmonE
	ZeekE
	MazeRunnerE
)

var Atomics = []Atomic{
	SuricataE,
	SyslogE,
	SnoopyE,
	EventLogE,
	SysmonE,
}

// Functions
// FixBrokenMessage applies replace operations to handle improper escape sequences, etc, that would otherwise result with error in unmarshal
// Because parsing unkown input is fun and all incoming data is always clean and formatted according to spec....
// Simply a hack that tries to handle known cases, not a by-the-book sanitizer!!!
// Should be used as fallback in case Unmarshal fails
// Should always return original input slice if something goes wrong
func TryFixBrokenMessage(data []byte) []byte {
	data = bytes.ReplaceAll(data, []byte(`\(`), []byte(`(`))
	data = bytes.ReplaceAll(data, []byte(`\)`), []byte(`)`))
	data = bytes.ReplaceAll(data, []byte(`\*`), []byte(`*`))
	return data
}

func getField(key string, data map[string]interface{}) (interface{}, bool) {
	if data == nil {
		return nil, false
	}
	bits := strings.SplitN(key, ".", 2)
	if val, ok := data[bits[0]]; ok {
		switch res := val.(type) {
		case map[string]interface{}:
			return getField(bits[1], res)
		default:
			return val, ok
		}
	}
	return nil, false
}

type EventMapFn func(string) Atomic
