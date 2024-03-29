package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go-peek/pkg/models/consumer"
	"go-peek/pkg/utils"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

var Version = "2.6.0"

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
			messages:    make(chan *consumer.Message, 0),
			logInterval: c.LogInterval,
			logger:      c.Logger,
			name:        c.Name,
		},
		errs: utils.NewErrChan(100, fmt.Sprintf(
			"kafka consumer for brokers %+v topics %+v",
			c.Brokers,
			c.Topics,
		)),
	}
	// TODO - make configurable
	// obj.config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	switch c.OffsetMode {
	case OffsetEarliest:
		obj.config.Consumer.Offsets.Initial = sarama.OffsetOldest
	case OffsetLatest:
		obj.config.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
	}
	version, err := sarama.ParseKafkaVersion(Version)
	if err != nil {
		return nil, err
	}
	obj.config.Version = version
	obj.config.Consumer.Return.Errors = true

	group, err := sarama.NewConsumerGroup(c.Brokers, c.ConsumerGroup, obj.config)
	if err != nil {
		return obj, err
	}
	obj.group = group
	obj.wg.Add(1)
	go func() {
		defer obj.wg.Done()
	loop:
		for {
			select {
			case <-obj.ctx.Done():
				break loop
			case err := <-obj.group.Errors():
				if err != nil {
					obj.errs.Send(err)
				}
			default:
				if err := obj.group.Consume(obj.ctx, c.Topics, obj.handle); err != nil {
					obj.errs.Send(err)
				}
			}
		}
	}()
	go func() {
		obj.wg.Wait()
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
	mu          sync.RWMutex
	consumed    map[string]int64
	messages    chan *consumer.Message
	logInterval time.Duration
	name        string
	logger      *logrus.Logger
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *handle) Setup(session sarama.ConsumerGroupSession) error {
	if c.logger != nil {
		c.logger.Tracef("Sarama consumer claimed: %+v", session.Claims())
	}
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *handle) Cleanup(sarama.ConsumerGroupSession) error {
	c.partitionLog()
	return nil
}

func (c *handle) consumeLog(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) {
	if c.logger != nil {
		c.logger.
			WithField("name", c.name).
			WithField("member_id", session.MemberID()).
			WithField("high_watermark_offset", claim.HighWaterMarkOffset()).
			WithField("topic", claim.Topic()).
			WithField("partition", claim.Partition()).
			Trace("sarama partition info")
	}
}

func (c *handle) partitionLog() {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.logger != nil && len(c.consumed) != 0 {
		fields := make(map[string]any)
		for k, v := range c.consumed {
			fields[k] = v
		}
		c.logger.WithFields(fields).Info("consumed from partitions")
	}
}

func (c *handle) updateConsumed(claim sarama.ConsumerGroupClaim) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.consumed == nil {
		c.consumed = make(map[string]int64)
	}
	c.consumed[fmt.Sprintf("%s-%d", claim.Topic(), claim.Partition())] = claim.HighWaterMarkOffset()
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *handle) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	logTick := time.NewTicker(c.logInterval)
	first := time.NewTimer(10 * time.Second)
loop:
	for {
		select {
		case <-first.C:
			c.consumeLog(session, claim)
			c.updateConsumed(claim)
		case <-logTick.C:
			c.consumeLog(session, claim)
			c.updateConsumed(claim)
		case msg, ok := <-claim.Messages():
			if !ok {
				break loop
			}
			c.messages <- &consumer.Message{
				Partition: int64(msg.Partition),
				Data:      msg.Value,
				Offset:    msg.Offset,
				Source:    msg.Topic,
				Time:      msg.Timestamp,
				Key:       string(msg.Key),
				Type:      consumer.Kafka,
			}
		}
	}
	return nil
}
