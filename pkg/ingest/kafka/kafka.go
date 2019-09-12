package kafka

import (
	"context"
	"sync"

	"github.com/Shopify/sarama"
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

type Config struct {
	Brokers             []string
	ConsumerGroup       string
	Topics              []string
	SaramaClusterConfig *cluster.Config
	Ctx                 context.Context
	OffsetMode          OffsetMode
	NoCommit            bool
}

func NewDefaultConfig() *Config {
	return &Config{
		Brokers:             []string{"localhost:9092"},
		ConsumerGroup:       "peek",
		Topics:              []string{},
		SaramaClusterConfig: newConsumerConfig(),
		Ctx:                 context.Background(),
	}
}

func (c *Config) Validate() error {
	if c == nil {
		c = NewDefaultConfig()
	}
	if c.SaramaClusterConfig == nil {
		c.SaramaClusterConfig = newConsumerConfig()
	}
	if c.Brokers == nil || len(c.Brokers) == 0 {
		c.Brokers = []string{"localhost:9092"}
	}
	if c.Topics == nil || len(c.Topics) == 0 {
		c.Topics = []string{}
	}
	if c.ConsumerGroup == "" {
		c.ConsumerGroup = "peek"
	}
	if c.Ctx == nil {
		c.Ctx = context.Background()
	}
	switch c.OffsetMode {
	case OffsetEarliest:
		c.SaramaClusterConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	case OffsetLatest:
		c.SaramaClusterConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
	}
	return nil
}

func newConsumerConfig() *cluster.Config {
	var config = cluster.NewConfig()
	config.Consumer.Return.Errors = false
	config.Group.Return.Notifications = false
	return config
}

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

func (c Consumer) Messages() <-chan *consumer.Message {
	return c.messages
}

func (c Consumer) Errors() <-chan error {
	return c.handle.Errors()
}
