package models

import (
	"net"
	"strconv"
	"time"
)

type ipNet struct{ net.IP }

func (n *ipNet) UnmarshalJSON(b []byte) error {
	s, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	ip, _, err := net.ParseCIDR(s)
	if err != nil {
		return err
	}
	n.IP = ip
	return nil
}

type AssetVcenter struct {
	TS          time.Time `json:"ts"`
	AnsibleName string    `json:"ansible_name"`
	HostName    string    `json:"host_name"`
	Domain      string    `json:"domain"`
	IP          *ipNet    `json:"ip"`
	MAC         string    `json:"mac"`
	Team        string    `json:"team"`
}
