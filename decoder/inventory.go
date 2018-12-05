package decoder

import "net"

type TargetInfo struct {
	ID     string
	Pretty string

	IPv4 []net.IP
	IPv6 []net.IP
	MAC  []net.HardwareAddr
}
