package events

import (
	"fmt"
	"testing"
)

func TestIP(t *testing.T) {
	inf := NewSource()
	inf.SetIpFromString("8.8.8.8")

	fmt.Println(len(inf.IP))
}

func TestInfo(t *testing.T) {
	inf := NewSource()
	inf.Host = "asd"
	inf.SetSrcDestNames("sourceHost", "destHost")

	from, _ := inf.Src.GetHost()
	if inf.Host != "asd" || from != "sourceHost" {
		t.Fail()
	}

	inf.SetSrcDestNames("newSourceHost", "")
}
