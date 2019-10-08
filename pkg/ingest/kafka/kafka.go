package kafka

import (
	"context"
	"sync"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	log "github.com/sirupsen/logrus"
)

type OffsetMode int

const (
	OffsetLastCommit OffsetMode = iota
	OffsetEarliest
	OffsetLatest
)

type Consumer struct {
	handle   *cluster.Consumer
	messages chan *consumer.Message
	ctx      context.Context
	active   bool
	wg       *sync.WaitGroup
}

func NewConsumer(c *Config) (*Consumer, error) {
	if c == nil {
		c = NewDefaultConfig()
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	obj := &Consumer{ctx: c.Ctx, wg: &sync.WaitGroup{}}
	if handle, err := cluster.NewConsumer(
		c.Brokers,
		c.ConsumerGroup,
		c.Topics,
		c.SaramaClusterConfig,
	); err != nil {
		return obj, err
	} else {
		obj.handle = handle
	}
	obj.messages = make(chan *consumer.Message, 0)

	obj.active = true
	obj.wg.Add(1)
	go func(ctx context.Context) {
		defer obj.wg.Done()
	loop:
		for obj.active {
			select {
			case msg, ok := <-obj.handle.Messages():
				if !ok {
					break loop
				}
				obj.messages <- &consumer.Message{
					Partition: int64(msg.Partition),
					Data:      msg.Value,
					Offset:    msg.Offset,
					Source:    msg.Topic,
					Time:      msg.Timestamp,
					Key:       string(msg.Key),
					Type:      consumer.Kafka,
				}
			case <-ctx.Done():
				obj.handle.Close()
			}
		}
		log.Trace("kafka consumer exited successfully")
	}(obj.ctx)

	go func() {
		obj.wg.Wait()
		obj.active = false
		close(obj.messages)
	}()

	return obj, nil
}

func (c Consumer) Messages() <-chan *consumer.Message { return c.messages }
func (c Consumer) Errors() <-chan error               { return c.handle.Errors() }
