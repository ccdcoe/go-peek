package atomic

import (
	"time"
)

type DynamicSuricataEve map[string]any

func (d DynamicSuricataEve) Time() time.Time {
	key := "@timestamp"
	if val, ok := d[key]; ok {
		switch v := val.(type) {
		case time.Time:
			return v
		case string:
			if ts, err := time.Parse(time.RFC3339, v); err == nil {
				return ts
			}
		}
	}
	return time.Time{}
}

func (d DynamicSuricataEve) Source() string {
	val, ok := d["event_type"].(string)
	if ok {
		return val
	}
	return ""
}

func (d DynamicSuricataEve) Sender() string {
	val, ok := d["host"].(string)
	if ok {
		return val
	}
	return ""
}
