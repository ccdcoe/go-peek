package meta

import (
	"encoding/json"
	"net"
)

// AssetGetterSetter is a collection of methods for enritching exercise events with meta information for correlation without explicitly knowing the item type in runtime
// meta object asset lookups sould be done externally after using GetGameMeta() to avoid copying large cache structures, invoking race conditions or lock contentions, etc
// collected meta should be then reattached using SetGameMeta to make processing logic easily readable in code
type AssetGetterSetter interface {
	// GetAsset is a getter for receiving event source and target information
	// For exampe, event source for syslog is usually the shipper, while suricata alert has affected source and destination IP addresses whereas directionality matters
	// Should provide needed information for doing external asset table lookups
	GetAsset() *GameAsset
	// SetAsset is a setter for setting meta to object without knowing the object type
	// all asset lookups and field discoveries should be done before using this method to maintain readability
	SetAsset(GameAsset)
}

type NetSegment struct {
	net.IPNet
	Name string `json:"name"`
	Desc string `json:"desc"`
}

type Indicators struct {
	IsAsset bool `json:"is_asset"`
	IsRogue bool `json:"is_rogue"`
	IsBad   bool `json:"is_bad"`
}

type Asset struct {
	Host   string `json:"Host"`
	Alias  string `json:"Alias,omitempty"`
	Kernel string `json:"Kernel,omitempty"`
	IP     net.IP `json:"IP"`
	Indicators
	NetSegment *NetSegment
}

func (a Asset) SetSegment(s *NetSegment) *Asset {
	a.NetSegment = s
	return &a
}

type GameAsset struct {
	Asset

	Directionality

	Source      *Asset `json:"Src"`
	Destination *Asset `json:"Dest"`
}

func (g *GameAsset) SetDirection() *GameAsset {
	if g.Source != nil && g.Destination != nil {
		if g.Source.IsAsset && !g.Destination.IsAsset {
			return g.SetOutbound()
		}
		if !g.Source.IsAsset && g.Destination.IsAsset {
			return g.SetInbound()
		}
		if g.Source.IsAsset && g.Destination.IsAsset {
			return g.SetLateral()
		}
	}
	if g.Source == nil && g.Destination == nil {
		return g.SetLocal()
	}
	return g
}

func (g *GameAsset) SetLateral() *GameAsset {
	g.Directionality = DirLateral
	return g
}
func (g *GameAsset) SetInbound() *GameAsset {
	g.Directionality = DirInbound
	return g
}
func (g *GameAsset) SetOutbound() *GameAsset {
	g.Directionality = DirOutbound
	return g
}
func (g *GameAsset) SetLocal() *GameAsset {
	g.Directionality = DirLocal
	return g
}

func (g GameAsset) JSON() ([]byte, error) { return json.Marshal(g) }

type Directionality int

const (
	// Fallback
	DirUnk Directionality = iota
	DirLocal
	DirLateral
	DirInbound
	DirOutbound
)
