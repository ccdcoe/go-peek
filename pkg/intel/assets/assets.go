package assets

import (
	"encoding/json"
	"go-peek/pkg/ingest/kafka"
	"go-peek/pkg/models/consumer"
	"go-peek/pkg/models/meta"
	"go-peek/pkg/utils"
	"sync"
	"time"
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

	msgs    chan *consumer.Message
	assetCh chan *Asset

	errs *utils.ErrChan

	data *sync.Map
	wg   *sync.WaitGroup
}

func NewHandle(c Config) (*Handle, error) {
	consumer, err := kafka.NewConsumer(&c.Kafka)
	if err != nil {
		return nil, err
	}
	h := &Handle{
		consumer: consumer,
		wg:       &sync.WaitGroup{},
		errs:     utils.NewErrChan(100, "asset json parse errors"),
		assetCh:  make(chan *Asset, 0),
	}
	return h, nil
}

func (h Handle) Errors() <-chan error {
	return h.errs.Items
}

func (h *Handle) consume() *Handle {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
	loop:
		for msg := range h.consumer.Messages() {
			var obj meta.RawAsset
			if err := json.Unmarshal(msg.Data, &obj); err != nil {
				h.errs.Send(err)
				continue loop
			}
			h.assetCh <- &Asset{
				Updated: time.Now(),
				Asset:   obj.Asset().Copy(),
			}
		}
	}()
	return h
}
