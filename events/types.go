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

type Source struct {
	Host, IP string

	Src  *Source
	Dest *Source
}

func (s *Source) SetSrcDestNames(src, dest string) *Source {
	if src != "" {
		if s.Src == nil {
			s.Src = &Source{}
		}
		s.Src.Host = src
	}
	if dest != "" {
		if s.Dest == nil {
			s.Dest = &Source{}
		}
		s.Dest.Host = dest
	}
	return s
}

func (s Source) GetSrcIp() (string, bool) {
	if s.Src != nil {
		return s.Src.IP, true
	}
	return s.IP, false
}
func (s Source) GetDestIp() (string, bool) {
	if s.Dest != nil {
		return s.Dest.IP, true
	}
	return s.IP, false
}
