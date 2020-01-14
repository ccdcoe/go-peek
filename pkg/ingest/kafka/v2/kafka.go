package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	"github.com/ccdcoe/go-peek/pkg/utils"
)

type Consumer struct {
	group  sarama.ConsumerGroup
	client *sarama.Client
	config *sarama.Config

	handle *handle
	errs   *utils.ErrChan

	ctx context.Context
	wg  *sync.WaitGroup
}

func NewConsumer(c *Config) (*Consumer, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	obj := &Consumer{
		ctx:    c.Ctx,
		wg:     &sync.WaitGroup{},
		config: sarama.NewConfig(),
		handle: &handle{
			messages: make(chan *consumer.Message, 0),
		},
		errs: utils.NewErrChan(100, fmt.Sprintf(
			"kafka consumer for brokers %+v topics %+v",
			c.Brokers,
			c.Topics,
		)),
	}
	// TODO - make configurable
	obj.config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	switch c.OffsetMode {
	case OffsetEarliest:
		obj.config.Consumer.Offsets.Initial = sarama.OffsetOldest
	case OffsetLatest:
		obj.config.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
	}
	version, err := sarama.ParseKafkaVersion("2.1.1")
	if err != nil {
		return nil, err
	}
	obj.config.Version = version

	group, err := sarama.NewConsumerGroup(c.Brokers, c.ConsumerGroup, obj.config)
	if err != nil {
		return obj, err
	}
	obj.group = group
	obj.wg.Add(1)
	go func() {
		defer obj.wg.Done()
		//defer obj.group.Close()
	loop:
		for {
			select {
			case <-obj.ctx.Done():
				break loop
			default:
				if err := obj.group.Consume(obj.ctx, c.Topics, obj.handle); err != nil {
					obj.errs.Send(err)
				}
			}
		}
	}()
	go func() {
		obj.wg.Wait()
		//obj.group.Close()
		close(obj.handle.messages)
	}()
	return obj, nil
}

func (c Consumer) Messages() <-chan *consumer.Message {
	return c.handle.messages
}
func (c Consumer) Errors() <-chan error {
	return c.errs.Items
}

// handle represents a Sarama consumer group consumer
type handle struct {
	messages chan *consumer.Message
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *handle) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *handle) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *handle) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for msg := range claim.Messages() {
		c.messages <- &consumer.Message{
			Partition: int64(msg.Partition),
			Data:      msg.Value,
			Offset:    msg.Offset,
			Source:    msg.Topic,
			Time:      msg.Timestamp,
			Key:       string(msg.Key),
			Type:      consumer.Kafka,
		}
		// TODO - autocommit?
		session.MarkMessage(msg, "")
	}

	return nil
}

type OffsetMode int

const (
	OffsetLastCommit OffsetMode = iota
	OffsetEarliest
	OffsetLatest
)
