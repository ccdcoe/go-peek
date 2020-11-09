package meta

import (
	"net"

	"go-peek/pkg/models/fields"
)

// NetworkPandasExport is for loading network table as exported from pandas to_json() method
type NetworkPandasExport struct {
	Name         map[int]string           `json:"Name"`
	Abbreviation map[int]string           `json:"Abbreviation"`
	VLAN         map[int]string           `json:"VLAN/Portgroup"`
	IPv4         map[int]fields.StringNet `json:"IPv4"`
	IPv6         map[int]fields.StringNet `json:"IPv6"`
	Desc         map[int]string           `json:"Description"`
	Whois        map[int]string           `json:"WHOIS"`
	Team         map[int]string           `json:"Team"`
}

func (n NetworkPandasExport) Extract() []*Network {
	list := make([]*Network, 0)
	for id, name := range n.Name {
		net := &Network{
			Name:         name,
			ID:           id,
			Abbreviation: n.Abbreviation[id],
			VLAN:         n.VLAN[id],
			IPv4:         n.IPv4[id].IPNet,
			IPv6:         n.IPv6[id].IPNet,
			Desc:         n.Desc[id],
			Whois:        n.Whois[id],
			Team:         n.Team[id],
		}
		list = append(list, net)
	}
	return list
}

// Network is a long representation of a segment, as used in exercise prep
type Network struct {
	ID           int       `json:"id"`
	Name         string    `json:"Name"`
	Abbreviation string    `json:"Abbreviation"`
	VLAN         string    `json:"VLAN/Portgroup"`
	IPv4         net.IPNet `json:"IPv4"`
	IPv6         net.IPNet `json:"IPv6"`
	Desc         string    `json:"Description"`
	Whois        string    `json:"WHOIS"`
	Team         string    `json:"Team"`
}

func (n Network) Shorthand() (*NetSegment, *NetSegment) {
	return &NetSegment{
			ID:   n.ID,
			net:  n.IPv4,
			Name: n.Name,
		}, &NetSegment{
			ID:   n.ID,
			net:  n.IPv6,
			Name: n.Name,
		}
}

// NetSegment is a shorthand representation of Network, more suitable to be used in de-normalized logging
type NetSegment struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	VLAN string `json:"vlan"`
	net  net.IPNet
}

func (n NetSegment) String() string          { return n.net.String() }
func (n NetSegment) Contains(ip net.IP) bool { return n.net.Contains(ip) }
