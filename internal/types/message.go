package types

import (
	"time"
)

type Messager interface {
	Messages() <-chan Message
}

type Message struct {
	// Raw message in byte array format
	// Can be original message or processed
	Data []byte

	// Message offset from input
	// e.g. kafka offset or file line number
	Offset int64

	// Textual representation of input source
	// e.g. source file, kafka topic, redis key, etc
	// can also be a hash if source path is too long
	Source string

	// Optional message key, separate from source topic
	// Internal from message, as opposed to external from topic
	// e.g. Kafka key, syslog program, suricata event type, eventlog channel, etc
	Key string

	// Optional timestamp from source message
	// Should default to time.Now() if timestamp is missing or not parsed
	Time time.Time

	// Optional reformatted messages to integrate with message-format sensitive tools
	// Map key should represent message format name (e.g. RFC5424, CEE, CEF )
	// Value should correspond to formatted message
	Formats map[string]string
}

// String is a helpter for accessing byte array payload in string format
func (m Message) String() string {
	return string(m.Data)
}

// MessageChannel implements Messager
type MessageChannel chan Message

// Messages method provides read-only version of chan Message
// Provides minimal interface for building a coherent message queue
func (m MessageChannel) Messages() <-chan Message {
	return m
}
