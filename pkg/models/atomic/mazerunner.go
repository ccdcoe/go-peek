package atomic

import "time"

type MazeRunner struct {
	Syslog
	Cef Cef `json:"cef,omitempty"`
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (m MazeRunner) Time() time.Time { return m.Syslog.Time() }

// Source implements atomic.Event
// Source of message, usually emitting program
func (m MazeRunner) Source() string { return m.Cef.Source() }

// Sender implements atomic.Event
// Sender of message, usually a host
func (m MazeRunner) Sender() string { return m.Cef.Sender() }

type Cef struct {
	DeviceVendor  string            `json:"DeviceVendor"`
	DeviceProduct string            `json:"DeviceProduct"`
	DeviceVersion string            `json:"DeviceVersion"`
	SignatureID   string            `json:"SignatureID"`
	Name          string            `json:"Name"`
	Severity      string            `json:"Severity"`
	Extensions    map[string]string `json:"Extensions"`
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (c Cef) Time() time.Time { return time.Time{} }

// Source implements atomic.Event
// Source of message, usually emitting program
func (c Cef) Source() string { return c.DeviceProduct }

// Sender implements atomic.Event
// Sender of message, usually a host
func (c Cef) Sender() string { return c.DeviceVendor }
