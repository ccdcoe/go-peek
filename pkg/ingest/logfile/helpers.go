package logfile

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var lineSep = []byte{'\n'}

type ErrUnknownGeneratorType struct {
	Item interface{}
}

func (e ErrUnknownGeneratorType) Error() string {
	return fmt.Sprintf("Unknown type for: %+v", e.Item)
}

type ErrEmptyCollect struct{}

func (e ErrEmptyCollect) Error() string {
	return fmt.Sprintf("Empty slice from generator")
}

func filePathGenerator(root string) <-chan interface{} {
	out := make(chan interface{}, 0)
	go func() {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				out <- err
			} else if !info.IsDir() {
				out <- path
			}
			return nil
		})
		if err != nil {
			out <- err
		}
		close(out)
	}()
	return out
}

func GenFileList(root string, breakOnErr bool) ([]Path, error) {
	var (
		gen = filePathGenerator(root)
		out = make([]Path, 0)
	)
	for res := range gen {
		switch pth := res.(type) {
		case error:
			if breakOnErr {
				return out, pth
			}
		case Path:
			out = append(out, pth)
		case string:
			out = append(out, Path(pth))
		default:
			return out, ErrUnknownGeneratorType{Item: pth}
		}
	}
	if len(out) == 0 {
		return out, ErrEmptyCollect{}
	}
	return out, nil
}

// Path represents a log file name with attached methods for common path manipulation, permission checking, etc
type Path string

func (h Path) String() string       { return string(h) }
func (h Path) Dir() string          { return filepath.Dir(h.String()) }
func (h Path) Ext() string          { return filepath.Ext(h.String()) }
func (h Path) Base() string         { return filepath.Base(h.String()) }
func (h Path) Abs() (string, error) { return filepath.Abs(h.String()) }
func (h Path) Clean() string        { return filepath.Clean(h.String()) }

func (h Path) Stat() (os.FileInfo, error) {
	handle, err := os.Open(h.String())
	if err != nil {
		return nil, err
	}
	defer handle.Close()
	return handle.Stat()
}

func statLogFileSinglePass(file io.Reader) (first, last []byte, lines int64, err error) {
	var (
		line  []byte
		count int64
	)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line = scanner.Bytes()
		slc := make([]byte, len(scanner.Bytes()))
		copy(slc, scanner.Bytes())
		if count == 0 {
			first = slc
		}
		count++
	}
	last = line
	return first, last, count, scanner.Err()
}

type ErrMissingParam struct {
	Param, Func string
}

func (e ErrMissingParam) Error() string {
	return fmt.Sprintf("Missing param %s for function %s", e.Param, e.Func)
}
