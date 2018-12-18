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
	Meta
	Src  *Meta `json:"Src"`
	Dest *Meta `json:"Dest"`
}

func NewSource() *Source {
	return &Source{
		Meta: Meta{},
	}
}

func (s *Source) NewSrcIfMissing() *Source {
	if !s.SrcIsSet() {
		s.Src = &Meta{}
	}
	return s
}
func (s *Source) NewDestIfMissing() *Source {
	if !s.DestIsSet() {
		s.Dest = &Meta{}
	}
	return s
}

func (s *Source) SetSrcIp(ip net.IP) *Source {
	s.NewSrcIfMissing().Src.SetIp(ip)
	return s
}

func (s *Source) SetDestIp(ip net.IP) *Source {
	s.NewDestIfMissing().Dest.SetIp(ip)
	return s
}

func (s *Source) SetSrcName(name string) *Source {
	s.NewSrcIfMissing().Src.SetHost(name)
	return s
}

func (s *Source) SetDestName(name string) *Source {
	s.NewDestIfMissing().Dest.SetHost(name)
	return s
}

func (s Source) GetSrcIp() (net.IP, bool) {
	if s.Src == nil {
		return nil, false
	}
	return s.Src.GetIp()
}

func (s Source) GetDestIp() (net.IP, bool) {
	if s.Dest == nil {
		return nil, false
	}
	return s.Dest.GetIp()
}

func (s Source) SrcIsSet() bool {
	if s.Src == nil {
		return false
	}
	return true
}
func (s Source) DestIsSet() bool {
	if s.Dest == nil {
		return false
	}
	return true
}

type Meta struct {
	Host string `json:"Host"`
	IP   net.IP `json:"IP"`
}

func (m Meta) GetIp() (net.IP, bool) {
	if len(m.IP) == 0 {
		return nil, false
	}
	return m.IP, true
}

func (m Meta) GetHost() (string, bool) {
	if m.Host == "" {
		return "", false
	}
	return m.Host, true
}

func (m *Meta) SetHost(host string) *Meta {
	if host != "" {
		m.Host = host
	}
	return m
}

func (m *Meta) SetIpFromString(ip string) *Meta {
	if ip != "" && len(m.IP) > 0 {
		m.IP = net.ParseIP(ip)
	}
	return m
}

func (m *Meta) SetIp(ip net.IP) *Meta {
	m.IP = ip
	return m
}
