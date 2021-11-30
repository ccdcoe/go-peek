package oracle

import (
	"encoding/json"
	"errors"
	"fmt"
	"go-peek/pkg/mitremeerkat"
	"go-peek/pkg/providentia"
	"sync"
)

type Assigner struct {
	ID int
}

func (a *Assigner) insertIoC(item IoC, container IoCMap, idm IoCMapID) (int, error) {
	key := item.key()
	if _, ok := container[key]; ok {
		return item.ID, nil
	}
	// assign new SID to IoC
	item = item.assign(a.ID)
	a.ID++
	// set IoC value
	ptr := &item
	container[key] = ptr
	// also add pointer to same item to ID map, so we can easily modify it
	idm[item.ID] = ptr
	// return new SID for API response
	return item.ID, nil
}

// DataIoC is for simple gob dumping without worrying about passing locks
type DataIoC struct {
	Assigner
	DestIP IoCMap
	SrcIP  IoCMap

	MapID IoCMapID
}

// ContainerIoC takes care of thread safety
type ContainerIoC struct {
	sync.RWMutex
	Data *DataIoC
}

func (c *ContainerIoC) Copy() DataIoC {
	c.RLock()
	defer c.RUnlock()
	return DataIoC{
		Assigner: c.Data.Assigner,
		SrcIP:    copyIocMap(c.Data.SrcIP),
		DestIP:   copyIocMap(c.Data.DestIP),
		MapID:    copyIoCMapID(c.Data.MapID),
	}
}

func (c *ContainerIoC) Disable(id int) (IoC, error) {
	c.Lock()
	defer c.Unlock()
	c.verify()
	item, ok := c.Data.MapID[id]
	if !ok {
		return IoC{}, fmt.Errorf("IoC with ID %d not found", id)
	}
	item.Enabled = false
	return *item, nil
}

func (c *ContainerIoC) Add(item IoC) (int, error) {
	c.Lock()
	defer c.Unlock()
	if err := item.validate(); err != nil {
		return -1, err
	}
	c.verify()
	switch item.Type {
	case "dest_ip":
		return c.Data.insertIoC(item, c.Data.DestIP, c.Data.MapID)
	case "src_ip":
		return c.Data.insertIoC(item, c.Data.SrcIP, c.Data.MapID)
	default:
	}
	return -1, errors.New("unsupported item type")
}

func (c *ContainerIoC) Slice() []IoC {
	c.RLock()
	defer c.RUnlock()
	c.verify()
	tx := make([]IoC, 0, len(c.Data.DestIP)+len(c.Data.SrcIP))
	tx = append(tx, c.Data.DestIP.Values()...)
	tx = append(tx, c.Data.SrcIP.Values()...)
	return tx
}

// helper with no lock, must be called internally by locked method
func (c *ContainerIoC) verify() {
	if c.Data == nil {
		c.Data = &DataIoC{}
	}
	if c.Data.DestIP == nil {
		c.Data.DestIP = make(IoCMap)
	}
	if c.Data.SrcIP == nil {
		c.Data.SrcIP = make(IoCMap)
	}
	if c.Data.MapID == nil {
		c.Data.MapID = make(IoCMapID)
	}
}

type ContainerMitreMeerkat struct {
	sync.RWMutex
	Data mitremeerkat.Mappings
}

func (c *ContainerMitreMeerkat) CSVFormat(header bool) [][]string {
	c.Lock()
	defer c.Unlock()
	return c.Data.CSVFormat(header)
}

func (c *ContainerMitreMeerkat) Update(d map[int]mitremeerkat.Mapping) {
	c.Lock()
	defer c.Unlock()
	if len(d) == 0 {
		return
	}
	c.Data = make([]mitremeerkat.Mapping, 0, len(d))
	for _, obj := range d {
		c.Data = append(c.Data, obj)
	}
}

func (c *ContainerMitreMeerkat) Copy(d mitremeerkat.Mappings) {
	c.Lock()
	defer c.Unlock()
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
	c.Lock()
	defer c.Unlock()

	if len(d) == 0 {
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
