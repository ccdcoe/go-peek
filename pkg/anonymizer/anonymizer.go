package anonymizer

import (
	"math/rand"
	"strings"
	"time"

	_ "embed"
)

//go:embed name_pool.txt
var pool []byte

type Pretty struct {
	Name   string
	Rename string

	FirstSeen time.Time
	LastSeen  time.Time
}

type Mapper struct {
	Data map[string]*Pretty
	Pool []string
}

func (m *Mapper) CheckAndUpdate(name string) string {
	if val, ok := m.Data[name]; ok && val.Rename != "" {
		val.LastSeen = time.Now()
		return val.Rename
	}
	idx := rand.Intn(len(m.Pool))

	pretty := &Pretty{
		Name:      name,
		Rename:    m.Pool[idx],
		FirstSeen: time.Now(),
		LastSeen:  time.Now(),
	}

	m.Data[name] = pretty
	m.Data[pretty.Rename] = pretty

	m.Pool = append(m.Pool[:idx], m.Pool[idx+1:]...)
	return pretty.Rename
}

func NewMapper() (*Mapper, error) {
	m := &Mapper{
		Pool: make([]string, 0),
		Data: make(map[string]*Pretty),
	}

	// TODO - load persitence here
	// TODO - badgerdb handler for persitence
	names := strings.Split(string(pool), "\n")
	for _, name := range names {
		// only load names that have not been used
		// both original and rename must be indexed
		if _, ok := m.Data[name]; !ok {
			m.Pool = append(m.Pool, name)
		}
	}
	return m, nil
}
