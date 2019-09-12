package atomic

import (
	"time"

	"github.com/ccdcoe/go-peek/pkg/models/fields"
)

type Syslog struct {
	Timestamp time.Time        `json:"@timestamp"`
	Host      string           `json:"syslog_host"`
	Program   string           `json:"syslog_program"`
	Severity  string           `json:"syslog_severity"`
	Facility  string           `json:"syslog_facility"`
	Message   string           `json:"syslog_message"`
	IP        *fields.StringIP `json:"syslog_ip,omitempty"`
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (s Syslog) Time() time.Time { return s.Timestamp }

// Source implements atomic.Event
// Source of message, usually emitting program
func (s Syslog) Source() string { return s.Program }

// Sender implements atomic.Event
// Sender of message, usually a host
func (s Syslog) Sender() string { return s.Host }
