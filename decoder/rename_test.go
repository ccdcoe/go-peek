package decoder

import (
	"fmt"
	"testing"
)

func TestRename(t *testing.T) {
	ren, err := NewRename("/tmp/rename.gob")
	if err != nil {
		t.Fatal(err)
	}
	pretty := ren.Check("my.awesome.server")
	fmt.Println(pretty)
	fmt.Println(ren.ByName)
}
