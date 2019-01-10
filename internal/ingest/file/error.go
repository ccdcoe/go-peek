package file

import (
	"bytes"
	"fmt"
)

type ErrUnsupportedFileMagic struct {
	File  string
	Magic []byte
}

func (e ErrUnsupportedFileMagic) Error() string {
	return fmt.Sprintf("Unsupported file magic for %s", e.File)
}

func (e ErrUnsupportedFileMagic) Rune() []rune {
	return bytes.Runes(e.Magic)
}
