package meta

import "net"

type Indicators struct {
	IsAsset bool `json:"is_asset"`
	IsRogue bool `json:"is_rogue"`
	IsBad   bool `json:"is_bad"`
}

type Asset struct {
	Host   string `json:"Host"`
	Alias  string `json:"Alias"`
	Kernel string `json:"Kernel"`
	IP     net.IP `json:"IP"`
	Indicators
	NetSegment *NetSegment
}

func (a *Asset) SetSegment(s *NetSegment) *Asset {
	a.NetSegment = s
	return a
}
