package events

import "encoding/json"

type Snoopy struct {
	Syslog
	Cmd      string `json:"cmd"`
	Filename string `json:"filename"`
	Cwd      string `json:"cwd"`
	Tty      string `json:"tty"`
	Sid      string `json:"sid"`
	Gid      string `json:"gid"`
	Group    string `json:"group"`
	UID      string `json:"uid"`
	Username string `json:"username"`
	SSH      struct {
		DstPort string `json:"dst_port"`
		DstIP   string `json:"dst_ip"`
		SrcPort string `json:"src_port"`
		SrcIP   string `json:"src_ip"`
	} `json:"ssh"`
	Login string `json:"login"`
}

func (s Snoopy) JSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s Snoopy) Source() Source {
	return Source{
		Host: s.Host,
		IP:   s.IP,
	}
}

func (s *Snoopy) Rename(pretty string) {
	s.Host = pretty
}
