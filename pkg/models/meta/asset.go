package meta

import "net"

type Indicators struct {
	IsAsset bool `json:"is_asset"`
}

type Asset struct {
	Host  string `json:"Host"`
	Alias string `json:"Alias"`
	OS    string `json:"OS"`
	VM    string `json:"VM"`
	IP    net.IP `json:"IP"`
	Indicators
}
