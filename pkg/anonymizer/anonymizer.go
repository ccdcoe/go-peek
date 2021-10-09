package anonymizer

import (
	"bytes"
	"encoding/gob"
	"go-peek/pkg/persist"
	"math/rand"
	"strings"
	"time"

	_ "embed"
)

const (
	PersistKeyPrefix = "pretty"
)

//go:embed name_pool.txt
var pool []byte

type Config struct {
	Persist *persist.Badger
}

type Pretty struct {
	Name   string
	Rename string

	FirstSeen time.Time
	LastSeen  time.Time
}

type Mapper struct {
	Data    map[string]*Pretty
	Pool    []string
	Persist *persist.Badger

	Hits, Misses int
}

func (m *Mapper) CheckAndUpdate(name string) string {
	if val, ok := m.Data[name]; ok && val.Rename != "" {
		m.Hits++
		val.LastSeen = time.Now()
		return val.Rename
	}
	m.Misses++
	idx := rand.Intn(len(m.Pool))

	pretty := &Pretty{
		Name:      name,
		Rename:    m.Pool[idx],
		FirstSeen: time.Now(),
		LastSeen:  time.Now(),
	}

	m.Data[pretty.Name] = pretty
	m.Data[pretty.Rename] = pretty

	m.Persist.Set(PersistKeyPrefix, persist.GenericValue{Key: pretty.Name, Data: pretty})
	m.Persist.Set(PersistKeyPrefix, persist.GenericValue{Key: pretty.Rename, Data: pretty})

	m.Pool = append(m.Pool[:idx], m.Pool[idx+1:]...)
	return pretty.Rename
}

func NewMapper(c Config) (*Mapper, error) {
	m := &Mapper{
		Pool:    make([]string, 0),
		Data:    make(map[string]*Pretty),
		Persist: c.Persist,
	}

	records := m.Persist.Scan(PersistKeyPrefix)
	for record := range records {
		var pretty Pretty
		buf := bytes.NewBuffer(record.Data)
		err := gob.NewDecoder(buf).Decode(&pretty)
		if err != nil {
			return nil, err
		}
		m.Data[pretty.Name] = &pretty
		m.Data[pretty.Rename] = &pretty
	}
	names := strings.Split(string(pool), "\n")
	for _, name := range names {
		// only load names that have not been used
		if _, ok := m.Data[name]; !ok {
			m.Pool = append(m.Pool, name)
		}
	}
	return m, nil
}
