package oracle

import (
	"encoding/json"
	"go-peek/pkg/mitremeerkat"
	"go-peek/pkg/providentia"
	"sync"
)

type ContainerMitreMeerkat struct {
	sync.RWMutex
	Data []mitremeerkat.Mapping
}

func (c *ContainerMitreMeerkat) Update(d map[int]mitremeerkat.Mapping) {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()
	if d == nil || len(d) == 0 {
		return
	}
	c.Data = make([]mitremeerkat.Mapping, 0, len(d))
	for _, obj := range d {
		c.Data = append(c.Data, obj)
	}
}

func (c *ContainerMitreMeerkat) Copy(d mitremeerkat.Mappings) {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()
	if d == nil || len(d) == 0 {
		return
	}
	c.Data = make(mitremeerkat.Mappings, len(d))
	copy(c.Data, d)
}

func (c *ContainerMitreMeerkat) JSONFormat() ([]byte, error) {
	c.RLock()
	defer c.RUnlock()
	if c.Data == nil {
		c.Data = make([]mitremeerkat.Mapping, 0)
	}
	return json.Marshal(c.Data)
}

type ContainerAssets struct {
	sync.RWMutex
	Data providentia.Records
}

func (c *ContainerAssets) Update(d map[string]providentia.Record) {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()

	if d == nil || len(d) == 0 {
		return
	}
	c.Data = make([]providentia.Record, 0, len(d))
	for _, obj := range d {
		c.Data = append(c.Data, obj)
	}
}

func (c *ContainerAssets) JSONFormat() ([]byte, error) {
	c.RLock()
	defer c.RUnlock()
	if c.Data == nil {
		c.Data = make([]providentia.Record, 0)
	}
	return json.Marshal(c.Data)
}
