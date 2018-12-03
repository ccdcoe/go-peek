package events

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/Jeffail/gabs"
)

type eventLogTs struct{ time.Time }

func (t *eventLogTs) UnmarshalJSON(b []byte) error {
	raw, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	t.Time, err = time.Parse("2006-01-02 15:04:05", raw)
	return err
}

//type DynaEventLog struct{ Vals *gabs.Container }
type DynaEventLog struct{ Vals *gabs.Container }

func NewDynaEventLog(raw []byte) (*DynaEventLog, error) {
	var parsed *gabs.Container
	var err error
	if parsed, err = gabs.ParseJSON(raw); err != nil {
		return nil, err
	}
	return &DynaEventLog{Vals: parsed}, nil
}

func (s DynaEventLog) JSON() ([]byte, error) {
	return s.Vals.Bytes(), nil
}

func (s DynaEventLog) Source() Source {
	var (
		name = s.Vals.Path("Hostname").Data().(string)
		ip   = s.Vals.Path("ip").Data().(string)
	)
	return Source{
		Host: name,
		IP:   ip,
	}
}

func (s *DynaEventLog) Rename(pretty string) {
	s.Vals.Set(pretty, "host")
	s.Vals.Set(pretty, "Hostname")
	s.Vals.DeleteP("ip")
}

type SimpleEventLog struct {
	Syslog

	EventTime *eventLogTs `json:"EventTime"`
	Channel   string      `json:"Channel"`
	Hostname  string      `json:"Hostname"`
}

func (s SimpleEventLog) JSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s SimpleEventLog) Source() Source {
	return Source{
		Host: s.Hostname,
		IP:   s.Host,
	}
}

func (s *SimpleEventLog) Rename(pretty string) {
	s.Host = pretty
	s.Hostname = pretty
}
