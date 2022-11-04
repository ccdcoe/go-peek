package oracle

import (
	"encoding/json"
	"errors"
	"fmt"
	"go-peek/pkg/mitremeerkat"
	"go-peek/pkg/providentia"
	"io"
	"sync"
)

type Assigner struct {
	ID int
}

func (a *Assigner) insertIoC(item IoC, container IoCMap, idm IoCMapID, set bool) (int, error) {
	key := item.key()
	if _, ok := container[key]; ok {
		return item.ID, nil
	}
	if !set {
		// assign new SID to IoC
		item = item.assign(a.ID)
		a.ID++
	} else {
		// offline persist load needs to fix assigner offset, or adding items later will cause
		// duplicate IDs
		if item.ID > a.ID {
			a.ID = item.ID + 1
		}
	}
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
	DestIP   IoCMap
	SrcIP    IoCMap
	JA3S     IoCMap
	JA3      IoCMap
	TLSSNI   IoCMap
	HTTPHost IoCMap

	MapID IoCMapID
}

// ContainerIoC takes care of thread safety
type ContainerIoC struct {
	sync.RWMutex
	Data *DataIoC
}

func (c *ContainerIoC) Offset() int {
	c.RLock()
	defer c.RUnlock()
	return c.Data.ID
}

func (c *ContainerIoC) Len() int {
	c.RLock()
	defer c.RUnlock()
	if c.Data.MapID == nil {
		return 0
	}
	return len(c.Data.MapID)
}

func (c *ContainerIoC) Extract() []IoC {
	c.Lock()
	defer c.Unlock()
	if c.Data == nil || c.Data.MapID == nil {
		return nil
	}
	tx := make([]IoC, 0, len(c.Data.MapID))
	for _, item := range c.Data.MapID {
		tx = append(tx, *item)
	}
	return tx
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

func (c *ContainerIoC) Enable(id int) (IoC, error) {
	c.Lock()
	defer c.Unlock()
	c.verify()
	item, ok := c.Data.MapID[id]
	if !ok {
		return IoC{}, fmt.Errorf("IoC with ID %d not found", id)
	}
	item.Enabled = true
	return *item, nil
}

func (c *ContainerIoC) Add(item IoC, set bool) (int, error) {
	c.Lock()
	defer c.Unlock()
	if err := item.validate(); err != nil {
		return -1, err
	}
	c.verify()
	switch item.Type {
	case "dest_ip":
		return c.Data.insertIoC(item, c.Data.DestIP, c.Data.MapID, set)
	case "src_ip":
		return c.Data.insertIoC(item, c.Data.SrcIP, c.Data.MapID, set)
	case "tls.ja3s.hash":
		return c.Data.insertIoC(item, c.Data.JA3S, c.Data.MapID, set)
	case "tls.ja3.hash":
		return c.Data.insertIoC(item, c.Data.JA3, c.Data.MapID, set)
	case "tls.sni":
		return c.Data.insertIoC(item, c.Data.TLSSNI, c.Data.MapID, set)
	case "http.hostname":
		return c.Data.insertIoC(item, c.Data.HTTPHost, c.Data.MapID, set)
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
	tx = append(tx, c.Data.JA3.Values()...)
	tx = append(tx, c.Data.JA3S.Values()...)
	tx = append(tx, c.Data.TLSSNI.Values()...)
	tx = append(tx, c.Data.HTTPHost.Values()...)
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
	if c.Data.JA3S == nil {
		c.Data.JA3S = make(IoCMap)
	}
	if c.Data.JA3 == nil {
		c.Data.JA3 = make(IoCMap)
	}
	if c.Data.HTTPHost == nil {
		c.Data.HTTPHost = make(IoCMap)
	}
	if c.Data.TLSSNI == nil {
		c.Data.TLSSNI = make(IoCMap)
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

func (c *ContainerAssets) FmtWISE(rw io.Writer) {
	c.RLock()
	defer c.RUnlock()
	fmt.Fprintf(rw, "#field:target.name;shortcut:0\n")
	fmt.Fprintf(rw, "#field:target.pretty;shortcut:1\n")
	fmt.Fprintf(rw, "#field:target.team;shortcut:2\n")
	fmt.Fprintf(rw, "#field:target.os;shortcut:3\n")
	fmt.Fprintf(rw, "#field:target.network_name;shortcut:4\n")
	for _, record := range c.Data {
		fmt.Fprintf(rw,
			"%s;0=%s;1=%s;2=%s;3=%s;4=%s\n",
			record.Addr.String(),
			record.FQDN,
			record.Pretty,
			record.Team,
			record.OS,
			record.NetworkName,
		)
	}
}
