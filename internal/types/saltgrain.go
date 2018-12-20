package types

import "net"

type SaltGrain struct {
	Host             string                 `json:"host"`
	Osrelease        string                 `json:"osrelease"`
	IPInterfaces     map[string][]*StringIP `json:"ip_interfaces"`
	IP6Interfaces    map[string][]*StringIP `json:"ip6_interfaces"`
	IP4Interfaces    map[string][]*StringIP `json:"ip4_interfaces"`
	HwaddrInterfaces map[string]string      `json:"hwaddr_interfaces"`
	Ipv6             []*StringIP            `json:"ipv6"`
	Ipv4             []*StringIP            `json:"ipv4"`
	FqdnIP4          []string               `json:"fqdn_ip4"`
	FqdnIP6          []interface{}          `json:"fqdn_ip6"`
	Fqdn             string                 `json:"fqdn"`
	Os               string                 `json:"os"`
}

func (s SaltGrain) GetV4() []*StringIP {
	var localhost = net.ParseIP("127.0.0.1")
	var addrs = make([]*StringIP, 0)
	for _, v := range s.Ipv4 {
		if !v.Equal(localhost) {
			addrs = append(addrs, v)
		}
	}
	return addrs
}

func (s SaltGrain) GetV6() []*StringIP {
	var ignore = net.ParseIP("::1")
	var addrs = make([]*StringIP, 0)
	for _, v := range s.Ipv6 {
		if !v.Equal(ignore) {
			addrs = append(addrs, v)
		}
	}
	return addrs
}

func (s SaltGrain) GetAddrs() AddrSlice {
	return append(s.GetV4(), s.GetV6()...)
}
