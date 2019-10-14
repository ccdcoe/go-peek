package consumer

/*
	consumer package is a data model
	modelled after Kafka, but extended to any input source
	for example, line number where event occurred can be considered an offset
*/

import (
	"net"
	"time"

	"github.com/ccdcoe/go-peek/pkg/models/events"
)

type Source int

func (s Source) String() string {
	switch s {
	case Kafka:
		return "kafka"
	case Logfile:
		return "logfile"
	default:
		return "NA"
	}
}

const (
	Unknown Source = iota
	Logfile
	Kafka
	UxSock
	Redis
)

type Messager interface {
	// Messages implements consumer.Messager
	Messages() <-chan *Message
}

// Message is an atomic log entry that is closely modeled after kafka event
type Message struct {
	// Raw message in byte array format
	// Can be original message or processed
	Data []byte

	// Message offset from input
	// e.g. kafka offset or file line number
	Offset int64

	// Message partition from input
	// in case messages are segregated
	Partition int64

	// Enum that maps to supported source module
	// Logfile, unix socket, kafka, redis
	Type Source

	// Enum that maps to message format
	// for example, suricata, syslog, eventlog, etc
	// for efficiently deciding which parser to use
	Event events.Atomic

	// Textual representation of input source
	// e.g. source file, kafka topic, redis key, etc
	// can also be a hash if source path is too long
	Source string

	// Optional message key, separate from source topic
	// Internal from message, as opposed to external from topic
	// e.g. Kafka key, syslog program, suricata event type, eventlog channel, etc
	Key string

	// Optional timestamp from source message
	// Can default to time.Now() if timestamp is missing or not parsed
	// Should default to time.Now() if message is consumed online
	Time time.Time

	// Optional sender IP address
	// For example, syslog UDP sender info is usually taken from UDP source
	Sender net.IP
}

type Offsets struct {
	Beginning, End int64
}

func (o Offsets) Len() int64 {
	return (o.End - o.Beginning) + 1
}

// TopicMapFn is a helper for allowing the user to define how individual messages should be handled
// For example, which elasticsearch index to send the message to whereas final index name requires knowledge of event timestamp
type TopicMapFn func(Message) string
