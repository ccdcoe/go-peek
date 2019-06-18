package types

import (
	"fmt"
	"time"
)

type MultiPlexorConfig struct {
	Input Messager
	Keys  []string
}

func (m MultiPlexorConfig) Validate() error {
	if m.Input == nil {
		return fmt.Errorf("cannot create message multiplexor, input missing")
	}
	if m.Keys == nil || len(m.Keys) == 0 {
		return fmt.Errorf("Cannot create message multiplexor, keys missing")
	}
	return nil
}

func MultiPlexMessages(config MultiPlexorConfig) (map[string]SimpleMessager, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	output := make(map[string]SimpleMessager)
	for _, v := range config.Keys {
		output[v] = make(SimpleMessager, len(config.Keys))
	}
	finally := func() {
		for _, v := range output {
			close(v)
		}
	}
	go func(mp map[string]SimpleMessager) {
		defer finally()
	loop:
		for {
			select {
			case msg, ok := <-config.Input.Messages():
				if !ok {
					break loop
				}
				for k := range mp {
					mp[k] <- msg
				}
			}
		}
	}(output)
	return output, nil
}

type SimpleMessager chan Message

func (s SimpleMessager) Messages() <-chan Message {
	return s
}

type Messager interface {
	Messages() <-chan Message
}

type Formats struct {
	Sagan string
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
	Formats
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
