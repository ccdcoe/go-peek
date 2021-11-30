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
	ID      int       `json:"id"`
	Enabled bool      `json:"enabled"`
	Value   string    `json:"value"`
	Type    string    `json:"type"`
	Added   time.Time `json:"added"`
}

func (i IoC) key() string {
	return i.Type + "_" + i.Value
}

func (i IoC) assign(id int) IoC {
	return IoC{
		Value:   i.Value,
		Type:    i.Type,
		Enabled: i.Enabled,
		Added:   time.Now(),
		ID:      id,
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

// IoCMapID stores IoC entries by ID for enable / disable
type IoCMapID map[int]*IoC

// IoCMap is for IoC entry to ensure unique item is created per type and value
type IoCMap map[string]*IoC

// Values is for reporting IoC list in GET and for exposing them to rule generator
func (i IoCMap) Values() []IoC {
	tx := make([]IoC, 0, len(i))
	for _, item := range i {
		tx = append(tx, *item)
	}
	return tx
}

func copyIocMap(rx IoCMap) IoCMap {
	tx := make(IoCMap)
	for key, value := range rx {
		cpy := *value
		tx[key] = &cpy
	}
	return tx
}

func copyIoCMapID(rx IoCMapID) IoCMapID {
	tx := make(IoCMapID)
	for key, value := range rx {
		cpy := *value
		tx[key] = &cpy
	}
	return tx
}
