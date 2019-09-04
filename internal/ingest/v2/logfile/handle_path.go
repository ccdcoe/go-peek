package logfile

import (
	"os"
	"path/filepath"
)

// Path represents a log file name with attached methods for common path manipulation, permission checking, etc
type Path string

func (h Path) String() string       { return string(h) }
func (h Path) Dir() string          { return filepath.Dir(h.String()) }
func (h Path) Ext() string          { return filepath.Ext(h.String()) }
func (h Path) Base() string         { return filepath.Base(h.String()) }
func (h Path) Abs() (string, error) { return filepath.Abs(h.String()) }

func (h Path) Stat() (os.FileInfo, error) {
	handle, err := os.Open(h.String())
	if err != nil {
		return nil, err
	}
	defer handle.Close()
	return handle.Stat()
}
