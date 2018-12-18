package events

import (
	"net"
	"strconv"
)

type stringIP struct{ net.IP }

func (t *stringIP) UnmarshalJSON(b []byte) error {
	raw, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	t.IP = net.ParseIP(raw)
	return err
}

type Meta struct {
	Host string `json:"Host"`
	IP   string `json:"IP"`
}

type Struct struct {
}

type Source struct {
	Host string  `json:"Host"`
	IP   string  `json:"IP"`
	Src  *Source `json:"Src"`
	Dest *Source `json:"Dest"`
}

func (s *Source) SetSrcDestNames(src, dest string) *Source {
	return s.SetSrcName(src).SetDestName(dest)
}

func (s *Source) SetSrcName(name string) *Source {
	if name != "" && s.Src != nil {
		s.Src.Host = name
	}
	return s
}

func (s *Source) SetDestName(name string) *Source {
	if name != "" && s.Dest != nil {
		s.Dest.Host = name
	}
	return s
}

func (s Source) GetSrcIp() (string, bool) {
	if s.Src != nil {
		if s.Src.IP == "" {
			return s.Src.IP, false
		}
		return s.Src.IP, true
	}
	return s.IP, false
}
func (s Source) GetDestIp() (string, bool) {
	if s.Dest != nil {
		if s.Dest.IP == "" {
			return s.Dest.IP, false
		}
		return s.Dest.IP, true
	}
	return s.IP, false
}
