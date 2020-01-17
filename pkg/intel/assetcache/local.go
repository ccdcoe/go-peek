package assetcache

import (
	"context"
	"math/rand"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// Local is a caching container that is meant to be performant but not thread safe
// Worker should ask from Global if entry is missing from map
type LocalCache struct {
	*sync.Mutex
	assets map[string]Asset
	parent *GlobalCache
	prune
	ctx     context.Context
	stopper context.CancelFunc
	id      int
}

func NewLocalCache(parent *GlobalCache, id int) *LocalCache {
	ctx, cancel := context.WithCancel(context.Background())
	lc := &LocalCache{
		id:      id,
		ctx:     ctx,
		stopper: cancel,
		Mutex:   &sync.Mutex{},
		assets:  make(map[string]Asset),
		parent:  parent,
		prune: prune{
			enabled:  true,
			interval: time.Duration(rand.Intn(3))*time.Second + 10*time.Second,
			period:   1 * time.Minute,
		},
	}
	if lc.prune.enabled {
		go func() {
			tick := time.NewTicker(lc.prune.interval)
		loop:
			for {
				select {
				case <-ctx.Done():
					break loop
				case <-tick.C:
					now := time.Now()
					lc.Lock()
					count := 0
					for k, v := range lc.assets {
						if now.Sub(v.updated) > lc.prune.period && !v.IsAsset {
							count++
							delete(lc.assets, k)
						}
					}
					total := len(lc.assets)
					lc.Unlock()
					log.Tracef(
						"pruned %d expired items from local asset cache for worker %d, now has %d items",
						count, lc.id, total)
				}
			}
			log.Tracef("worker %d cache pruner stopped", lc.id)
		}()
	}
	return lc
}

func (l LocalCache) GetString(key string) (*Asset, bool) {
	if l.assets == nil {
		return nil, false
	}
	l.Lock()
	defer l.Unlock()
	if val, ok := l.assets[key]; ok {
		return &val, true
	}
	if l.parent != nil {

		if val, ok := l.parent.GetString(key); ok {
			l.assets[key] = *val
			return val, true
		} else if !ok && val != nil {
			l.assets[key] = *val
		}

	}
	return nil, false
}
func (l LocalCache) GetIP(key string) (*Asset, bool) {
	if l.assets == nil {
		return nil, false
	}
	l.Lock()
	defer l.Unlock()
	if val, ok := l.assets[key]; ok {
		return &val, true
	}
	if l.parent != nil {

		if val, ok := l.parent.GetString(key); ok {
			l.assets[key] = *val
			return val, true
		} else if !ok && val != nil {
			l.assets[key] = *val
		}

	}
	return nil, false
}

func (l *LocalCache) Close() error {
	if l.stopper != nil {
		l.stopper()
	}
	return nil
}
