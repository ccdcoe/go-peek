package atomic

import "time"

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
