package enrich

import (
	"encoding/json"
	"go-peek/pkg/models/events"
)

type Config struct {
}

func (c Config) Validate() error {
	return nil
}

type Counts struct {
	Events      uint
	MissingMeta uint

	countsParseErrs
}

type countsParseErrs struct {
	Suricata uint
	Windows  uint
	Syslog   uint
	Snoopy   uint
}

type Handler struct {
	Counts
}

func (h *Handler) Decode(raw []byte, kind events.Atomic) (events.GameEvent, error) {
	var event events.GameEvent
	h.Counts.Events++

	switch kind {
	case events.SuricataE:
		var obj events.Suricata
		if err := json.Unmarshal(raw, &obj); err != nil {
			h.Counts.countsParseErrs.Suricata++
			return nil, err
		}
		event = &obj
	case events.EventLogE, events.SysmonE:
		var obj events.DynamicWinlogbeat
		if err := json.Unmarshal(raw, &obj); err != nil {
			h.Counts.countsParseErrs.Windows++
			return nil, err
		}
		event = &obj
	case events.SyslogE:
		var obj events.Syslog
		if err := json.Unmarshal(raw, &obj); err != nil {
			h.Counts.countsParseErrs.Syslog++
			return nil, err
		}
		event = &obj
	case events.SnoopyE:
		var obj events.Snoopy
		if err := json.Unmarshal(raw, &obj); err != nil {
			h.Counts.countsParseErrs.Snoopy++
			return nil, err
		}
		event = &obj
	}
	return event, nil
}

func (h *Handler) Enrich(event events.GameEvent) error {
	return nil
}

func (h *Handler) Close() error {
	return nil
}

func NewHandler(c Config) (*Handler, error) {
	h := &Handler{}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return h, nil
}
