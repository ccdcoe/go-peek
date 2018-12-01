package events

import "encoding/json"

type Unknown map[string]interface{}
type EventLog struct {
	Syslog
	Hostname string `json:"Hostname"`
	Unknown
}

func (s EventLog) JSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s EventLog) Source() Source {
	return Source{
		Host: s.Hostname,
		IP:   s.IP,
	}
}

func (s *EventLog) Rename(pretty string) {
	s.Host = pretty
	s.Hostname = pretty
}
