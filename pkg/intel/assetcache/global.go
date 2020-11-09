package assetcache

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"go-peek/pkg/intel/wise"
	"go-peek/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type persist struct {
	dump     time.Duration
	assets   string
	networks string
}

// Global is a caching container that is meant to be thread safe
// should ask from external sources if entry is missing
type GlobalCache struct {
	assets *sync.Map

	persist

	prune
	wise    *wise.Handle
	ctx     context.Context
	stopper context.CancelFunc
	wg      *sync.WaitGroup

	Errs *utils.ErrChan
}

func NewGlobalCache(c *Config) (*GlobalCache, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	gc := &GlobalCache{
		assets: &sync.Map{},
		prune: prune{
			enabled: func() bool {
				if c.Prune {
					return true
				}
				return false
			}(),
			interval: 30 * time.Second,
			period:   120 * time.Second,
		},
		ctx:     ctx,
		stopper: cancel,
		wg:      &sync.WaitGroup{},
		Errs:    utils.NewErrChan(100, "Global asset cache errors"),
		persist: persist{
			dump: 5 * time.Second,
		},
	}
	go func() {
		log.Tracef("spawning global asset cache housekeeper thread")
		gc.wg.Add(1)
		defer gc.wg.Done()
	loop:
		for {
			tick := time.NewTicker(gc.prune.interval)
			dump := time.NewTicker(gc.persist.dump)
			select {
			case <-gc.ctx.Done():
				break loop
			case <-tick.C:
				if !gc.prune.enabled {
					continue loop
				}
				log.Tracef("global assset cache pruning executed")
				now := time.Now()
				var count, total int
				gc.assets.Range(func(k, v interface{}) bool {
					switch a := v.(type) {
					// TODO - interface
					case Asset:
						if now.Sub(a.updated) > gc.prune.period && !a.IsAsset {
							gc.assets.Delete(k)
							count++
						}
					case *Asset:
						if now.Sub(a.updated) > gc.prune.period && !a.IsAsset {
							gc.assets.Delete(k)
							count++
						}
					default:
					}
					total++
					return true
				})
				log.Tracef(
					"pruned %d expired items from global asset cache, now has %d items",
					count, total)
			case <-dump.C:
				if gc.persist.assets == "" {
					continue loop
				}
				log.Tracef("dumping assets to %s", gc.persist.assets)
				stuff := make([]Asset, 0)
				gc.assets.Range(func(k, v interface{}) bool {
					switch a := v.(type) {
					case Asset:
						if a.IsAsset {
							stuff = append(stuff, a)
						}
					case *Asset:
						if a.IsAsset {
							stuff = append(stuff, *a)
						}
					}
					return true
				})
				if len(stuff) == 0 {
					log.Trace("No stuff to dump, continuing")
					continue loop
				}
				f, err := os.Create(gc.persist.assets)
				if err != nil {
					gc.Errs.Send(err)
					continue loop
				}
				for _, a := range stuff {
					j, err := a.JSON()
					if err != nil {
						panic(err)
					}
					if err == nil {
						fmt.Fprintf(f, "%s\n", string(j))
					}
				}
				f.Close()
			}
		}
		log.Tracef("global asset cache housekeeper exited correctly")
	}()
	if c.Wise != nil {
		handle, err := wise.NewHandle(c.Wise)
		gc.wise = handle
		if err != nil {
			return gc, err
		}
	}
	return gc, nil
}
func (g *GlobalCache) Close() error {
	if g.stopper != nil {
		g.stopper()
	}
	if g.wg != nil {
		g.wg.Wait()
	}
	return nil
}

func (g GlobalCache) GetString(key string) (*Asset, bool) {
	if g.assets == nil {
		return nil, false
	}
	if val, ok := g.assets.Load(key); ok {
		switch v := val.(type) {
		case Asset:
			return &v, true
		case *Asset:
			return v, true
		}
	}

	asset := &Asset{updated: time.Now()}
	if g.wise == nil {
		return asset, false
	}
	if a, ok, err := wise.GetAsset(
		*g.wise,
		key,
		FieldPrefix+".original",
		FieldPrefix+".pretty",
		FieldPrefix+".os",
		FieldPrefix+".vm",
	); err != nil {
		g.Errs.Send(err)
	} else if ok {
		asset.Data = a
		asset.IsAsset = true
	}
	g.assets.Store(key, asset)
	return asset, false
}
