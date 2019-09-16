package meta

import "net"

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

type Asset struct {
	Host       string `json:"Host"`
	Alias      string `json:"Alias,omitempty"`
	IP         net.IP `json:"IP"`
	NetSegment string `json:"Segment,omitempty"`
}

type GameAsset struct {
	Asset

	Directionality
	//MitreID int

	Source      *Asset `json:"Src"`
	Destination *Asset `json:"Dest"`
}

type Directionality int

const (
	// Fallback
	DirUnk Directionality = iota
	DirLocal
	DirLateral
	DirInbound
	DirOutbound
)
