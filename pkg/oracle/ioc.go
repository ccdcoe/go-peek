package oracle

import (
	"errors"
	"net"
	"time"
)

var (
	ErrMissingIoCType  = errors.New("missing IoC type")
	ErrMissingIoCValue = errors.New("missing IoC value")
	ErrInvalidIP       = errors.New("invalid IP addr")
)

type IndicatorOfCompromise int

// IoC stands for Indicator of compromise
type IoC struct {
	ID    int
	Value string
	Type  string
	Added time.Time
}

func (i IoC) key() string {
	return i.Type + "_" + i.Value
}

func (i IoC) assign(id int) IoC {
	return IoC{
		Value: i.Value,
		Type:  i.Type,
		Added: time.Now(),
		ID:    id,
	}
}

func (i IoC) validate() error {
	if i.Type == "" {
		return ErrMissingIoCType
	}
	if i.Value == "" {
		return ErrMissingIoCValue
	}
	switch i.Type {
	case "src_ip", "dest_ip":
		if addr := net.ParseIP(i.Value); addr == nil {
			return ErrInvalidIP
		}
	}
	return nil
}

type IoCMap map[string]IoC

func (i IoCMap) Values() []IoC {
	tx := make([]IoC, 0, len(i))
	for _, item := range i {
		tx = append(tx, item)
	}
	return tx
}
