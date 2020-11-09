package replay

import (
	"encoding/json"

	"go-peek/pkg/models/events"
	"go-peek/pkg/utils"
)

// ParseJSONTime implements utils.StatFileIntervalFunc
// events and ingest libraries are not supposed to know each other implementations as log stream may come in variety of different formats
// thus, interface functions are used as arguments when running tasks such as parsing first and last timestamps
func getIntervalFromJSON(first, last []byte) (utils.Interval, error) {
	var (
		i    = &utils.Interval{}
		b, e events.KnownTimeStamps
	)
	if err := json.Unmarshal(first, &b); err != nil {
		return *i, err
	}
	i.Beginning = b.Time()
	if err := json.Unmarshal(last, &e); err != nil {
		return *i, err
	}
	i.End = e.Time()
	return *i, nil
}
