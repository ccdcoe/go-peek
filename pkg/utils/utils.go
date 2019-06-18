package utils

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"time"
)

func GobLoadFile(path string, object interface{}) error {
	var (
		err  error
		file io.ReadCloser
	)
	if file, err = os.Open(path); err != nil {
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	if err = decoder.Decode(object); err != nil {
		return err
	}
	return nil
}

func GobSaveFile(path string, object interface{}) error {
	var (
		err  error
		file io.WriteCloser
	)
	if file, err = os.Create(path); err != nil {
		return err
	}
	defer file.Close()

	var encoder = gob.NewEncoder(file)
	if err = encoder.Encode(object); err != nil {
		return err
	}
	return nil
}

type Interval struct {
	Beginning, End time.Time
}

func NewIntervalFromStrings(start, stop, format string) (*Interval, error) {
	from, err := time.Parse(format, start)
	if err != nil {
		return nil, err
	}
	to, err := time.Parse(format, stop)
	if err != nil {
		return nil, err
	}
	if !from.Before(to) {
		return nil, fmt.Errorf("invalid interval from > to")
	}
	return &Interval{
		Beginning: from,
		End:       to,
	}, nil
}

func (i Interval) Unpack() (time.Time, time.Time) {
	return i.Beginning, i.End
}
