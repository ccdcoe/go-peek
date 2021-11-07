package models

import (
	"net"
	"time"
)

type AssetVcenter struct {
	TS          time.Time `json:"ts"`
	AnsibleName string    `json:"ansible_name"`
	HostName    string    `json:"host_name"`
	Domain      string    `json:"domain"`
	IP          net.IP    `json:"ip"`
	MAC         string    `json:"mac"`
	Team        string    `json:"team"`
}
