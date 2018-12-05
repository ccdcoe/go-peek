package events

import "net"

// GameMeta represents metadata about targest, parsed from ground-truth tables
type GameMeta struct {
	Iter  string `json:"iter"`
	Topic string `json:"topic"`
	Host  string

	Src  *GameSrcDest `json:"src"`
	Dest *GameSrcDest `json:"dest"`
}

type GameSrcDest struct {
	IP     net.IP
	Pretty string
}

type GameShipper struct {
	IPv4, IPv6 net.IP
	Management net.IP

	Pretty string
}
