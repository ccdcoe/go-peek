package assets

import (
	"context"
	"encoding/json"
	"go-peek/pkg/ingest/kafka"
	"go-peek/pkg/models/meta"
	"go-peek/pkg/utils"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"
)

const RedisStoreKey = "assets-peek"

type Config struct {
	Kafka kafka.Config
	Redis *redis.Options
}

type Asset struct {
	meta.Asset
	Updated time.Time
}

type Handle struct {
	consumer *kafka.Consumer
	redis    *redis.Client

	errs *utils.ErrChan

	dataByIP   *sync.Map
	dataByFQDN *sync.Map
	dataByHost *sync.Map

	missingHostnames *sync.Map

	config kafka.Config

	wg *sync.WaitGroup
}

func NewHandle(c Config) (*Handle, error) {
	consumer, err := kafka.NewConsumer(&c.Kafka)
	if err != nil {
		return nil, err
	}
	redisConn, err := func() (*redis.Client, error) {
		if c.Redis == nil {
			return nil, nil
		}
		r := redis.NewClient(c.Redis)
		if _, err := r.Ping().Result(); err != nil {
			return r, err
		}
		return r, nil
	}()
	if err != nil {
		return nil, err
	}
	h := &Handle{
		consumer:         consumer,
		wg:               &sync.WaitGroup{},
		errs:             utils.NewErrChan(100, "asset json parse errors"),
		dataByIP:         &sync.Map{},
		dataByFQDN:       &sync.Map{},
		dataByHost:       &sync.Map{},
		missingHostnames: &sync.Map{},
		redis:            redisConn,
	}
	if h.redis != nil {
		logrus.Trace("loading cached assets from redis")
		if err = h.loadAssetsFromRedis(); err != nil {
			return nil, err
		}
	}
	h.consume(context.TODO())
	return h, nil
}

func (h *Handle) SetErrs(errs *utils.ErrChan) *Handle {
	h.errs = errs
	return h
}

func (h Handle) Errors() <-chan error {
	return h.errs.Items
}

func (h Handle) GetIP(addr string) (*meta.Asset, bool) {
	if val, ok := h.dataByIP.Load(addr); ok {
		a := val.(Asset).Asset.Copy()
		return &a, true
	}
	return nil, false
}

func (h Handle) GetHost(hostname string) (*meta.Asset, bool) {
	if val, ok := h.dataByFQDN.Load(hostname); ok {
		a := val.(Asset).Asset.Copy()
		h.missingHostnames.Delete(hostname)
		return &a, true
	}
	h.missingHostnames.Store(hostname, true)
	return nil, false
}

func (h Handle) getHostFromRedis(hostname string) (*meta.Asset, bool) {
	raw, err := h.redis.Get(hostname).Bytes()
	if err == redis.Nil {
		h.missingHostnames.Store(hostname, true)
		return nil, false
	} else if err != nil {
		h.errs.Send(err)
		return nil, false
	}
	var asset Asset
	if err := json.Unmarshal(raw, &asset); err != nil {
		h.errs.Send(err)
		return nil, false
	}
	logrus.Tracef("got %s from redis", asset.Host)
	h.storeAsset(asset.Asset)

	return &asset.Asset, false
}

func (h Handle) storeAssetsInRedis(assets []Asset) error {
	data, err := json.Marshal(assets)
	if err != nil {
		return err
	}
	logrus.Tracef("Storing %d assets in redis key %s", len(assets), RedisStoreKey)
	return h.redis.Set(RedisStoreKey, data, 0).Err()
}

func (h Handle) loadAssetsFromRedis() error {
	raw, err := h.redis.Get(RedisStoreKey).Bytes()
	if err != nil && err != redis.Nil {
		return err
	}
	var assets []Asset
	if err := json.Unmarshal(raw, &assets); err != nil {
		return err
	}
	logrus.Tracef("got %d cached assets from redis key %s", len(assets), RedisStoreKey)
	for _, asset := range assets {
		h.storeAsset(asset.Asset)
	}
	return nil
}

func (h Handle) GetMissingHosts() []string {
	out := make([]string, 0)
	h.missingHostnames.Range(func(key, value interface{}) bool {
		out = append(out, key.(string))
		return true
	})
	return out
}

func (h Handle) GetAllHosts() map[string]Asset {
	out := make(map[string]Asset)
	h.dataByFQDN.Range(func(key, value interface{}) bool {
		out[key.(string)] = value.(Asset)
		return true
	})
	return out
}

func (h *Handle) consume(ctx context.Context) *Handle {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		report := time.NewTicker(5 * time.Second)
		redisDump := time.NewTicker(30 * time.Second)
		var count int64
	loop:
		for {
			select {
			case msg, ok := <-h.consumer.Messages():
				if !ok {
					break loop
				}
				if err := h.parseAndStoreAsset(msg.Data); err != nil {
					h.errs.Send(err)
					continue loop
				}
				count++
			case <-redisDump.C:
				if h.redis == nil {
					logrus.Warn("asset cache redis persist disabled")
				} else {
					h.storeAssetsInRedis(func() []Asset {
						tx := make([]Asset, 0)
						h.dataByIP.Range(func(key, value interface{}) bool {
							tx = append(tx, value.(Asset))
							return true
						})
						return tx
					}())
				}
			case <-report.C:
				logrus.Tracef("Asset consumer picked up %d items", count)
			}
		}
	}()
	return h
}

func (h *Handle) parseAndStoreAsset(raw []byte) error {
	var obj meta.RawAsset
	if err := json.Unmarshal(raw, &obj); err != nil {
		return err
	}
	h.storeAsset(obj.Asset().Copy())
	return nil
}

func (h *Handle) storeAsset(asset meta.Asset) *Handle {
	h.dataByIP.Store(asset.IP.String(), Asset{
		Updated: time.Now(),
		Asset:   asset.Copy(),
	})
	h.dataByFQDN.Store(asset.FQDN(), Asset{
		Updated: time.Now(),
		Asset:   asset.Copy(),
	})
	h.dataByHost.Store(asset.Host, Asset{
		Updated: time.Now(),
		Asset:   asset.Copy(),
	})
	return h
}
