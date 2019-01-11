package file

import (
	"testing"
)

func TestMagic(t *testing.T) {
	files := map[string]string{
		"gzip": "../../../test/logs/test.gz",
		"xz":   "../../../test/logs/test.xz",
	}
	for code, file := range files {
		magic, err := FileMagic(file)
		if err != nil {
			t.Fatal(err)
		}
		if code != magic {
			t.Errorf("expected %s got %s for %s", code, magic, file)
		}
	}
}
