package meta

import "net"

type Indicators struct {
	IsAsset bool `json:"is_asset"`
}

type Asset struct {
	Host   string `json:"Host"`
	Alias  string `json:"Alias"`
	OS     string `json:"OS"`
	IP     net.IP `json:"IP"`
	Zone   string `json:"Zone"`
	Team   string `json:"Team"`
	Domain string `json:"Domain"`
	VM     string `json:"VM"`
	Role   string `json:"Role"`

	Indicators
}

func (a Asset) Copy() Asset {
	return Asset{
		Host:   a.Host,
		Alias:  a.Alias,
		OS:     a.OS,
		IP:     net.ParseIP(a.IP.String()),
		Zone:   a.Zone,
		Team:   a.Team,
		Domain: a.Domain,
		VM:     a.VM,
		Role:   a.Role,
		Indicators: Indicators{
			IsAsset: a.Indicators.IsAsset,
		},
	}
}

type RawAsset struct {
	HostName string `json:"hostname"`
	Pretty   string `json:"pretty"`
	Role     string `json:"role"`
	OS       string `json:"os"`
	IP       net.IP `json:"ip"`
	Zone     string `json:"zone"`
	Team     string `json:"team"`
	Domain   string `json:"domain"`
	VM       string `json:"vm"`
}

func (r RawAsset) Asset() Asset {
	return Asset{
		Host:   r.HostName,
		Alias:  r.Pretty,
		Role:   r.Role,
		OS:     r.OS,
		IP:     net.ParseIP(r.IP.String()),
		Zone:   r.Zone,
		Team:   r.Team,
		Domain: r.Domain,
		VM:     r.VM,
	}
}
