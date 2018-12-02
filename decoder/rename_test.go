package decoder

import (
	"fmt"
	"testing"
)

func TestRename(t *testing.T) {
	ren, err := NewRename("/tmp/")
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	pretty := ren.Check("my.awesome.server")
	fmt.Println(pretty)
	fmt.Println(ren.ByName)
	fmt.Println(ren.NameSet.Get(pretty))
}
