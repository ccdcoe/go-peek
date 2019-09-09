package events

import "time"

type Simple map[string]interface{}

func (s Simple) Time() time.Time {
	return time.Now()
}
