package atomic

import "time"

// EventLog is a wrapper around EventLog to avoid parsing common fields in runtime
// DynamicEventLog methods incur full type cast and parse whenever fields are required, bad for performance
type EventLog struct {
	DynamicEventLog
	time           time.Time
	source, sender string
}

func (e *EventLog) Parse() *EventLog {
	if e.DynamicEventLog == nil || len(e.DynamicEventLog) == 0 {
		return e
	}
	e.time = e.DynamicEventLog.Time()
	e.source = e.DynamicEventLog.Source()
	e.sender = e.DynamicEventLog.Sender()
	return e
}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (s EventLog) Time() time.Time { return s.time }

// Source implements atomic.Event
// Source of message, usually emitting program
func (s EventLog) Source() string { return s.source }

// Sender implements atomic.Event
// Sender of message, usually a host
func (s EventLog) Sender() string { return s.sender }

// DynamicEventLog is meant to handle windows log messages without losing sanity
// it is okay to invoke full parse and typecast whenever a field is required via method
// use the results of these methods in EventLog if performance is needed
type DynamicEventLog map[string]interface{}

// Time implements atomic.Event
// Timestamp in event, should default to time.Time{} so time.IsZero() could be used to verify success
func (e DynamicEventLog) Time() time.Time {
	var key string
	switch e.Source() {
	case "Microsoft-Windows-Sysmon":
		key = "EventReceivedTime"
	default:
		key = "EventTime"
	}
	if val, ok := e[key]; ok {
		switch v := val.(type) {
		case time.Time:
			return v
		case string:
			if ts, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
				return ts
			}
		}
	}
	return time.Time{}
}

// Source implements atomic.Event
// Source of message, usually emitting program
func (e DynamicEventLog) Source() string {
	if val, ok := e["SourceName"].(string); ok {
		return val
	}
	return ""
}

// Sender implements atomic.Event
// Sender of message, usually a host
func (e DynamicEventLog) Sender() string {
	if val, ok := e["Host"].(string); ok {
		return val
	}
	return ""
}
