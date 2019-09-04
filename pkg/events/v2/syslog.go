package events

import (
	"time"
)

type Syslog struct {
	Timestamp time.Time `json:"@timestamp"`
	Host      string    `json:"syslog_host"`
	Program   string    `json:"syslog_program"`
	Severity  string    `json:"syslog_severity"`
	Facility  string    `json:"syslog_facility"`
	Message   string    `json:"syslog_message"`
	IP        *stringIP `json:"syslog_ip,omitempty"`
}

func (s Syslog) Time() time.Time {
	return s.Timestamp
}
