package anonymizer

import (
	"bytes"
	"encoding/gob"
	"errors"
	"go-peek/pkg/persist"
	"math/rand"
	"strings"
	"time"

	_ "embed"

	"github.com/sirupsen/logrus"
)

var ErrEmptyPool = errors.New("rename pool is empty")

const (
	PersistKeyPrefix = "pretty"
)

//go:embed name_pool.txt
var pool []byte

type Config struct {
	Persist *persist.Badger
	Logger  *logrus.Logger
}

type Pretty struct {
	Name   string
	Rename string

	FirstSeen time.Time
	LastSeen  time.Time
}

type Mapper struct {
	Data    map[string]*Pretty
	Pool    map[string]bool
	Persist *persist.Badger

	Hits, Misses int

	logger *logrus.Logger
}

func (m *Mapper) CheckAndUpdate(name string) (string, error) {
	if val, ok := m.Data[name]; ok && val.Rename != "" {
		m.Hits++
		val.LastSeen = time.Now()
		return val.Rename, nil
	}
	m.Misses++
	if len(m.Pool) == 0 {
		return "", ErrEmptyPool
	}
	idx := rand.Intn(len(m.Pool))
	var (
		rename string
		offset int
	)

loop:
	for key := range m.Pool {
		if offset == idx {
			rename = key
			delete(m.Pool, key)
			break loop
		}
		offset++
	}

	pretty := &Pretty{
		Name:      name,
		Rename:    rename,
		FirstSeen: time.Now(),
		LastSeen:  time.Now(),
	}

	m.Data[pretty.Name] = pretty
	m.Data[pretty.Rename] = pretty

	m.Persist.Set(PersistKeyPrefix, persist.GenericValue{Key: pretty.Name, Data: pretty})
	m.Persist.Set(PersistKeyPrefix, persist.GenericValue{Key: pretty.Rename, Data: pretty})

	if m.logger != nil {
		m.logger.WithFields(logrus.Fields{
			"name":      name,
			"alias":     rename,
			"idx":       idx,
			"offset":    offset,
			"pool_size": len(m.Pool),
		}).Trace("anonymizer name not found")
	}

	return pretty.Rename, nil
}

func NewMapper(c Config) (*Mapper, error) {
	m := &Mapper{
		Pool:    make(map[string]bool),
		Data:    make(map[string]*Pretty),
		Persist: c.Persist,
		logger:  c.Logger,
	}

	records := m.Persist.Scan(PersistKeyPrefix)
	var count int
	for record := range records {
		var pretty Pretty
		buf := bytes.NewBuffer(record.Data)
		err := gob.NewDecoder(buf).Decode(&pretty)
		if err != nil {
			return nil, err
		}
		m.Data[pretty.Name] = &pretty
		m.Data[pretty.Rename] = &pretty
		count++
	}
	if m.logger != nil {
		m.logger.WithField("count", count).Trace("anonymizer persist scan")
	}
	var poolCount struct {
		InUse     int
		Available int
	}
	names := strings.Split(string(pool), "\n")
	for _, name := range names {
		// only load names that have not been used
		if _, ok := m.Data[name]; !ok {
			m.Pool[name] = true
			poolCount.Available++
		} else {
			poolCount.InUse++
		}
	}
	if m.logger != nil {
		m.logger.WithFields(logrus.Fields{
			"pool_inuse":     poolCount.InUse,
			"pool_available": poolCount.Available,
			"pool_len":       len(m.Pool),
			"pool_names":     len(names),
		}).Trace("rename pool setup")
	}
	return m, nil
}
