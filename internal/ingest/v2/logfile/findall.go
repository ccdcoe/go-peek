package logfile

import (
	"fmt"
	"os"
	"path/filepath"
)

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

func genFileList(root string, breakOnErr bool) ([]Path, error) {
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
