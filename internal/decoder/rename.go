package decoder

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ccdcoe/go-peek/internal/types"
	"github.com/ccdcoe/go-peek/pkg/utils"
)

const names = "https://raw.githubusercontent.com/hadley/data-baby-names/master/baby-names.csv"

type ErrTypeAssert struct {
	expected string
	value    string
	key      string
	function string
}

func (e ErrTypeAssert) Error() string {
	return fmt.Sprintf(
		"Type assert fail in %s. Expected %s. Key: %s. Val: %s",
		e.function,
		e.expected,
		e.key,
		e.value,
	)
}

type Dump struct {
	Dir, Names, Mappings string
}

func (d Dump) CheckDir() error {
	if stat, err := os.Stat(d.Dir); err != nil {
		return err
	} else if !stat.IsDir() {
		return fmt.Errorf("requested path %s is not directory", d.Dir)
	}
	return nil
}

func (d Dump) NamePath() string {
	return filepath.Join(d.Dir, d.Names)
}
func (d Dump) MappingPath() string {
	return filepath.Join(d.Dir, d.Mappings)
}

func NameSetFromCSV(src io.Reader) (map[string]bool, error) {
	var (
		reader  = csv.NewReader(src)
		lines   = 0
		nameSet = make(map[string]bool)
	)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		// Expects a specific CSV structure
		if len(record) < 4 {
			return nil, fmt.Errorf("Rename set csv length wrong from %s", names)
		}
		if record[3] == "girl" {
			nameSet[record[1]] = true
		}
		lines++
	}
	if len(nameSet) == 0 {
		return nameSet, fmt.Errorf("empty name set. Check input source %s", names)
	}
	return nameSet, nil
}

type NameMappings struct {
	NamePool  *sync.Map
	NameReMap *sync.Map

	LenAvailableName int

	Dump
	*sync.Mutex
}

func (m NameMappings) DumpNames() map[string]string {
	mappings := map[string]string{}
	m.NameReMap.Range(func(k, v interface{}) bool {
		key, kok := k.(string)
		val, vok := v.(string)
		if kok && vok {
			mappings[key] = val
		}
		return true
	})
	return mappings
}

func newSyncNamePool(
	spooldir string,
	targetConf types.ElaTargetInventoryConfig,
) (*NameMappings, error) {
	var err error
	m := &NameMappings{
		NamePool:  &sync.Map{},
		NameReMap: &sync.Map{},
		Dump: Dump{
			Dir:      spooldir,
			Names:    "names.gob",
			Mappings: "mappings.gob",
		},
		Mutex: &sync.Mutex{},
	}

	if err = m.CheckDir(); err != nil {
		return nil, err
	}
	if err = m.loadOrGenerateNameSet(); err != nil {
		return nil, err
	}
	if err = m.loadNameMappingsIfDumped(); err != nil {
		return nil, err
	}
	if err = m.updateInventory(targetConf); err != nil {
		return nil, err
	}

	// *TODO* add logger for error check on dumps and updates
	go func(ctx context.Context, config types.ElaTargetInventoryConfig) {
		dump := time.NewTicker(1 * time.Minute)
		update := time.NewTicker(30 * time.Second)
	loop:
		for {
			select {
			case <-dump.C:
				m.Lock()
				saveNameSetToGob(m.NamePath(), m.NamePool)
				m.Unlock()
			case <-update.C:
				m.updateInventory(config)
			case <-ctx.Done():
				break loop
			}
		}
	}(context.Background(), targetConf)

	return m, nil
}

func (m *NameMappings) updateInventory(config types.ElaTargetInventoryConfig) (err error) {
	inventory := &types.ElaTargetInventory{}
	if err := inventory.Get(config); err != nil {
		return err
	}
	if inventory.TimedOut || inventory.Shards.Failed > 0 {
		return fmt.Errorf(
			"inventory update against %s %s timed out or got failed shards",
			strings.Join(config.Hosts, ","),
			config.Index,
		)
	}
	if inventory.Hits.Hits == nil || len(inventory.Hits.Hits) == 0 {
		return fmt.Errorf(
			"no grains from %s %s",
			strings.Join(config.Hosts, ","),
			config.Index,
		)
	}
	for _, v := range inventory.Hits.Hits {
		if v.Source == nil {
			return fmt.Errorf("grain missing for %s id: %s", v.Index, v.ID)
		}

		// Get addrs from inventory
		addrs := v.Source.GetAddrs()
		if val, ok := m.checkAddrList(addrs); ok {
			if err = m.bulkSetAddrs(addrs, val); err != nil {
				return err
			}
		} else {
			// Get random name from sync map
			i := rand.Intn(m.LenAvailableName)
			var name string
			m.NamePool.Range(func(k, v interface{}) bool {
				if i == 0 {
					name, ok = k.(string)
					if !ok {
						name = defaultName
					}
					return true
				}
				i--
				return true
			})
			if name != defaultName {
				if err = m.bulkSetAddrs(addrs, name); err != nil {
					return err
				}
				m.LenAvailableName--
				m.NamePool.Delete(name)
			}
		}
	}
	m.Lock()
	defer m.Unlock()
	if err = saveNameMappingsToGob(m.MappingPath(), m.NameReMap); err != nil {
		return err
	}
	return nil
}

func (m NameMappings) checkAddrList(addrs types.AddrSlice) (string, bool) {
	for _, addr := range addrs {
		if val, ok := m.NameReMap.Load(addr.String()); ok {
			val, ok := val.(string)
			if ok {
				return val, ok
			}
		}
	}
	return "", false
}

func (m *NameMappings) bulkSetAddrs(addrs types.AddrSlice, val string) error {
	var err error
	for _, key := range addrs {
		strkey := key.String()
		newval, loaded := m.NameReMap.LoadOrStore(strkey, val)
		if loaded {
			asserted, ok := newval.(string)
			if !ok {
				return &ErrTypeAssert{
					expected: "string",
					function: "build rename list from ip addrs",
					key:      strkey,
					value:    val,
				}
			}
			if asserted != val {
				return fmt.Errorf("key: %s expected: %s got: %s", strkey, val, asserted)
			}
		}
	}
	if err != nil {
		return err
	}
	return nil
}

func (m *NameMappings) loadOrGenerateNameSet() (err error) {
	if _, err = os.Stat(m.NamePath()); os.IsNotExist(err) {
		if m.NamePool, err = newSyncNameSetFromHTTP(); err != nil {
			return err
		}
		if err = saveNameSetToGob(m.NamePath(), m.NamePool); err != nil {
			return err
		}
	} else {
		if m.NamePool, err = loadNameSetFromGob(m.NamePath()); err != nil {
			return err
		}
	}
	m.NamePool.Range(func(k, v interface{}) bool {
		m.LenAvailableName++
		return true
	})
	return nil
}

func (m *NameMappings) loadNameMappingsIfDumped() (err error) {
	if _, err = os.Stat(m.MappingPath()); !os.IsNotExist(err) {
		var (
			nameSyncMap sync.Map
			rawdump     map[string]string
		)
		if err = utils.GobLoadFile(m.MappingPath(), &rawdump); err != nil {
			return err
		}
		for k, v := range rawdump {
			nameSyncMap.Store(k, v)
		}
		m.NameReMap = &nameSyncMap
	}
	return nil
}

func loadNameSetFromGob(path string) (*sync.Map, error) {
	var (
		nameSyncMap sync.Map
		rawdump     map[string]bool
	)
	if err := utils.GobLoadFile(path, &rawdump); err != nil {
		return nil, err
	}
	if len(rawdump) == 0 {
		return nil, fmt.Errorf(
			"loaded name set from %s but resulted in empty map",
			path,
		)
	}
	for k, v := range rawdump {
		nameSyncMap.Store(k, v)
	}
	return &nameSyncMap, nil
}

func saveNameSetToGob(path string, names *sync.Map) error {
	rawdump := map[string]bool{}
	var (
		key   string
		value bool
		ok    bool
		err   error
	)
	names.Range(func(k, v interface{}) bool {
		if key, ok = k.(string); !ok {
			err = &ErrTypeAssert{
				expected: "string",
				function: fmt.Sprintf("save name set to gob %s", path),
			}
			return false
		}
		if value, ok = v.(bool); !ok {
			err = &ErrTypeAssert{
				expected: "bool",
				function: fmt.Sprintf("save name set to gob %s", path),
				key:      key,
			}
			return false
		}
		rawdump[key] = value
		return true
	})
	if err != nil {
		return err
	}
	if err = utils.GobSaveFile(path, rawdump); err != nil {
		return err
	}
	return nil
}

func saveNameMappingsToGob(path string, names *sync.Map) error {
	rawdump := map[string]string{}
	var (
		key   string
		value string
		ok    bool
		err   error
	)
	names.Range(func(k, v interface{}) bool {
		if key, ok = k.(string); !ok {
			err = &ErrTypeAssert{
				expected: "string",
				function: fmt.Sprintf("save name set to gob %s", path),
			}
			return false
		}
		if value, ok = v.(string); !ok {
			err = &ErrTypeAssert{
				expected: "string",
				function: fmt.Sprintf("save name set to gob %s", path),
				key:      key,
			}
			return false
		}
		rawdump[key] = value
		return true
	})
	if err != nil {
		return err
	}
	if err = utils.GobSaveFile(path, rawdump); err != nil {
		return err
	}
	return nil
}

func newSyncNameSetFromHTTP() (*sync.Map, error) {
	var nameSyncMap sync.Map
	resp, err := http.Get(names)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	set, err := NameSetFromCSV(resp.Body)
	if err != nil {
		return nil, err
	}
	for k, v := range set {
		nameSyncMap.Store(k, v)
	}
	return &nameSyncMap, nil
}
