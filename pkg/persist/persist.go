package persist

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/sirupsen/logrus"
)

const (
	TokenPrefixJoin = "-"
)

var (
	ErrMissingTickDuration = errors.New("Missing tick duration for GC cleanup")
	ErrMissingHandle       = errors.New("Missing badgerDB handle")
	ErrNoValsToSet         = errors.New("Missing values for Set()")
	ErrMissingKey          = errors.New("Missing badgerDB entry key")
)

type Config struct {
	Directory string
	Logger    *logrus.Logger

	WaitGroup *sync.WaitGroup
	Ctx       context.Context

	IntervalGC    time.Duration
	RunValueLogGC bool
}

type GenericValue struct {
	Key  string
	Data interface{}
}

type ByteValue struct {
	Key  string
	Data []byte
}

func (v GenericValue) key(prefix string) ([]byte, error) {
	if v.Key == "" {
		return nil, ErrMissingKey
	}
	if prefix != "" {
		return []byte(prefix + TokenPrefixJoin + v.Key), nil
	}
	return []byte(v.Key), nil
}

type Badger struct {
	DB *badger.DB

	config Config
}

func (b Badger) Scan(prefix string) <-chan ByteValue {
	tx := make(chan ByteValue, 0)

	go func(bPrefix []byte) {
		var count int
		defer close(tx)
		if err := b.DB.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(bPrefix); it.ValidForPrefix(bPrefix); it.Next() {
				item := it.Item()
				err := item.Value(func(v []byte) error {
					k := strings.TrimLeft(string(item.Key()), prefix)
					k = strings.TrimLeft(k, TokenPrefixJoin)
					slc := make([]byte, len(v))
					copy(slc, v)
					tx <- ByteValue{
						Key:  k,
						Data: slc,
					}
					count++
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		}); err != nil && b.config.Logger != nil {
			b.config.Logger.Error(err)
		}

		if logger := b.config.Logger; logger != nil {
			logger.WithFields(logrus.Fields{
				"prefix": prefix, "count": count,
			}).Trace("badger prefix scan done")
		}
	}([]byte(prefix))

	return tx
}

func (b Badger) Set(prefix string, vals ...GenericValue) error {
	if vals == nil || len(vals) == 0 {
		return ErrNoValsToSet
	}
	if b.DB == nil {
		return ErrMissingHandle
	}
	if logger := b.config.Logger; logger != nil {
		if items := len(vals); items > 1 {
			logger.WithFields(logrus.Fields{
				"prefix": prefix, "count": items, "key": vals[0].Key,
			}).Tracef("Set badger entry")
		} else if items == 1 {
			logger.WithFields(logrus.Fields{
				"prefix": prefix, "count": 1, "key": vals[0].Key,
			}).Tracef("Set badger entry")
		}
	}
	txn := b.DB.NewTransaction(true)
	defer txn.Discard()

	if err := setLoop(txn, prefix, vals...); err != nil {
		return err
	}

	return txn.Commit()
}

type ValueHandleFunc func([]byte) error

func (b Badger) GetSingle(key string, handler ValueHandleFunc) error {
	if b.DB == nil {
		return ErrMissingHandle
	}
	return b.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		return item.Value(handler)
	})
}

func (b Badger) SetSingle(key string, value interface{}) error {
	if b.DB == nil {
		return ErrMissingHandle
	}
	if value == nil {
		return ErrNoValsToSet
	}
	return b.DB.Update(func(txn *badger.Txn) error {
		var buf bytes.Buffer
		encoder := gob.NewEncoder(&buf)
		err := encoder.Encode(value)
		if err != nil {
			return err
		}
		return txn.Set([]byte(key), buf.Bytes())
	})
}

func (b Badger) Cleanup() error {
	if b.DB == nil {
		return ErrMissingHandle
	}
	return b.DB.RunValueLogGC(0.5)
}

func (b Badger) Close() error {
	if b.DB == nil {
		return ErrMissingHandle
	}
	// TODO - stop cleanup routine if running
	return b.DB.Close()
}

func (b Badger) runCleanup(wg *sync.WaitGroup, ctx context.Context) error {
	if wg != nil {
		wg.Add(1)
	}
	if b.config.IntervalGC == 0 {
		return ErrMissingTickDuration
	}
	if b.DB == nil {
		return ErrMissingHandle
	}
	go func(tick *time.Ticker) {
		defer tick.Stop()
		if wg != nil {
			defer wg.Done()
		}
		if b.config.Logger != nil {
			defer func() {
				b.config.Logger.Trace("badgerdb cleanup routine exited")
			}()
		}
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case <-tick.C:
				err := b.Cleanup()
				if b.config.Logger != nil {
					if err == nil {
						b.config.Logger.Trace("Called badgerdb cleanup")
					} else {
						b.config.Logger.Error(err)
					}
				}
			}
		}
	}(time.NewTicker(b.config.IntervalGC))
	return nil
}

// NewBadger is a constructor
func NewBadger(c Config) (*Badger, error) {
	opts := badger.DefaultOptions(c.Directory).
		WithNumLevelZeroTables(1).
		WithNumLevelZeroTablesStall(2).
		WithLogger(c.Logger)

	conf := &Config{}

	// main params
	conf.Directory = c.Directory
	conf.Logger = c.Logger

	// cleanup params
	if c.Ctx == nil {
		conf.Ctx = context.Background()
	}
	conf.WaitGroup = c.WaitGroup

	if c.IntervalGC == 0 {
		c.IntervalGC = 1 * time.Minute
	}
	conf.RunValueLogGC = c.RunValueLogGC

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	handle := &Badger{
		DB:     db,
		config: c,
	}
	if c.RunValueLogGC {
		handle.runCleanup(c.WaitGroup, c.Ctx)
	}
	return handle, nil
}

// helper functions
func setLoop(
	txn *badger.Txn,
	prefix string,
	vals ...GenericValue,
) error {
	for _, val := range vals {
		var buf bytes.Buffer
		encoder := gob.NewEncoder(&buf)
		key, err := val.key(prefix)
		if err != nil {
			return err
		}
		err = encoder.Encode(val.Data)
		if err != nil {
			return err
		}
		err = txn.Set(key, buf.Bytes())
		if err != nil {
			return err
		}
		buf.Reset()
	}
	return nil
}
