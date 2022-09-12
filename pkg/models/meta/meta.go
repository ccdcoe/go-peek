package meta

import (
	"encoding/json"

	"github.com/markuskont/go-sigma-rule-engine"
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
	SetAsset(*GameAsset)
}

type GameAsset struct {
	Asset

	EventType string `json:"EventType"`

	DirectionString string `json:"DirectionString"`
	Directionality  `json:"Directionality"`

	MitreAttack  *MitreAttack  `json:"MitreAttack"`
	SigmaResults sigma.Results `json:"SigmaResults"`

	EventData *EventData `json:"EventData"`

	Source      *Asset `json:"Src"`
	Destination *Asset `json:"Dest"`
}

func (g *GameAsset) SetDirection() *GameAsset {
	switch {
	case g.Source == nil && g.Destination == nil:
		g.Directionality = DirLocal
	case g.Source.IsAsset && !g.Destination.IsAsset:
		g.Directionality = DirOutbound
	case !g.Source.IsAsset && g.Destination.IsAsset:
		g.Directionality = DirInbound
	case g.Source.IsAsset && g.Destination.IsAsset:
		g.Directionality = DirLateral
	default:
		g.Directionality = DirUnk
	}
	g.DirectionString = g.Directionality.String()
	return g
}

func (g *GameAsset) SetNetPivot() *GameAsset {
	g.Directionality = DirNetPivot
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

type Direction struct {
	ID   Directionality
	Desc string
}

type Directionality int

const (
	// Fallback
	DirUnk Directionality = iota
	DirLocal
	DirLateral
	DirInbound
	DirOutbound
	DirNetPivot
)

func (d Directionality) String() string {
	switch d {
	case DirLateral:
		return "Lateral"
	case DirLocal:
		return "Local"
	case DirInbound:
		return "Inbound"
	case DirOutbound:
		return "Outbound"
	case DirNetPivot:
		return "Network Pivot"
	default:
		return "Unknown"
	}
}

type EventDataDumper interface {
	// DumpEventData implements EventDataDumper
	DumpEventData() *EventData
}

// EventData contains core info fields from event
// For quickly filtering events in other scripts without having to access event-specific fields
// For example: Suricata signature ID and name, sysmon/snoopy captured commands, etc
type EventData struct {
	ID     int      `json:"ID"`
	Key    string   `json:"Key"`
	Fields []string `json:"Fields"`
}
