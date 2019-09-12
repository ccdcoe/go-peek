package meta

import "net"

type Asset struct {
	Host  string `json:"Host"`
	Alias string `json:"Alias,omitempty"`
	IP    net.IP `json:"IP"`
}

type Game struct {
	Asset

	Source      *Asset `json:"Src"`
	Destination *Asset `json:"Dest"`
}
