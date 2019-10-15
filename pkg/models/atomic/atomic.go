package atomic

import (
	"time"
)

type Event interface {
	// Time implements atomic.Event
	// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
	Time() time.Time
	// Source implements atomic.Event
	// Source of message, usually emitting program
	Source() string
	// Sender implements atomic.Event
	// Sender of message, usually a host
	Sender() string
}

// Messager is for accessing raw unstructure or human-formatted payload of event
// Useful for simple logging for humans or for text mining methods
type Messager interface {
	// Message implements atomic.Messager
	// For example, syslog msg field or Suricata EVE fields in fast log format
	Message() string
}

// JSONFormatter is for re-encoding parsed messages to byte array without knowng the payload type
type JSONFormatter interface {
	// JSONFormat implements atomic.JSONFormatter by wrapping json.Marshal
	JSONFormat() ([]byte, error)
}

// CSVFormatter is for encoding parsed messages in CSV format, for easy database dumps and excel stuff
// Output should integrate easily with encoding/csv Write() method
type CSVFormatter interface {
	CSVHeader() []string
	CSVFormat() []string
}
