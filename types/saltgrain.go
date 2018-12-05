package types

import "net"

type SaltGrain struct {
	Host             string                 `json:"host"`
	Osrelease        string                 `json:"osrelease"`
	IPInterfaces     map[string][]net.IP    `json:"ip_interfaces"`
	IP6Interfaces    map[string][]*StringIP `json:"ip6_interfaces"`
	IP4Interfaces    map[string][]*StringIP `json:"ip4_interfaces"`
	HwaddrInterfaces map[string][]string    `json:"hwaddr_interfaces"`
	Ipv6             []*StringIP            `json:"ipv6"`
	Ipv4             []*StringIP            `json:"ipv4"`
	FqdnIP4          []string               `json:"fqdn_ip4"`
	FqdnIP6          []interface{}          `json:"fqdn_ip6"`
	Fqdn             string                 `json:"fqdn"`
	Os               string                 `json:"os"`
}
