package assets

import (
	"context"
	"encoding/json"
	"fmt"
	"go-peek/pkg/ingest/kafka"
	"go-peek/pkg/models/meta"
	"go-peek/pkg/utils"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type Config struct {
	Kafka kafka.Config
}

type Asset struct {
	meta.Asset
	Updated time.Time
}

type Handle struct {
	consumer *kafka.Consumer

	errs *utils.ErrChan

	dataByIP   *sync.Map
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
	fmt.Println(c.Kafka)
	h := &Handle{
		consumer:         consumer,
		wg:               &sync.WaitGroup{},
		errs:             utils.NewErrChan(100, "asset json parse errors"),
		dataByIP:         &sync.Map{},
		dataByHost:       &sync.Map{},
		missingHostnames: &sync.Map{},
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
	if val, ok := h.dataByHost.Load(hostname); ok {
		a := val.(Asset).Asset.Copy()
		h.missingHostnames.Delete(hostname)
		return &a, true
	}
	h.missingHostnames.Store(hostname, true)
	return nil, false
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
	h.dataByHost.Range(func(key, value interface{}) bool {
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
		var count int64
	loop:
		for {
			select {
			case msg, ok := <-h.consumer.Messages():
				if !ok {
					break loop
				}
				var obj meta.RawAsset
				if err := json.Unmarshal(msg.Data, &obj); err != nil {
					h.errs.Send(err)
					continue loop
				}
				a := obj.Asset().Copy()
				h.dataByIP.Store(a.IP.String(), Asset{
					Updated: time.Now(),
					Asset:   a.Copy(),
				})
				var fqdn string
				if a.Domain != "" {
					fqdn = fmt.Sprintf("%s.%s", a.Host, a.Domain)
				} else {
					fqdn = a.Host
				}
				h.dataByHost.Store(fqdn, Asset{
					Updated: time.Now(),
					Asset:   a.Copy(),
				})
				count++
			case <-report.C:
				logrus.Tracef("Asset consumer picked up %d items", count)
			}
		}
	}()
	return h
}
