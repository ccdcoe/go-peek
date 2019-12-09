package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	log "github.com/sirupsen/logrus"
)

// Config is used as parameter when instanciating new Producer instance
type Config struct {
	Brokers      []string
	SaramaConfig *sarama.Config
	// TODO - automatically close producer if all feeders exit
	AutoClose bool
}

func NewDefaultConfig() *Config {
	return &Config{
		Brokers: []string{"localhost:9092"},
	}
}

func (c *Config) Validate() error {
	if c == nil {
		c = NewDefaultConfig()
	}
	if c.Brokers == nil || len(c.Brokers) == 0 {
		c.Brokers = []string{"localhost:9092"}
	}
	if c.SaramaConfig == nil {
		c.SaramaConfig = newProducerConfig()
	}
	return nil
}

type Producer struct {
	handle   sarama.AsyncProducer
	config   *sarama.Config
	active   bool
	feeders  *sync.WaitGroup
	errCount int
}

func NewProducer(c *Config) (*Producer, error) {
	if c == nil {
		c = NewDefaultConfig()
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	h := &Producer{config: c.SaramaConfig, feeders: &sync.WaitGroup{}}
	if producer, err := sarama.NewAsyncProducer(c.Brokers, c.SaramaConfig); err != nil {
		return nil, err
	} else {
		h.handle = producer
	}
	h.active = true

	// TODO - better producer error handling
	go func() {
	loop:
		for h.active {
			select {
			case _, ok := <-h.handle.Errors():
				if !ok {
					break loop
				}
				h.errCount++
			}
		}
	}()

	return h, nil
}

// Feed implements outputs.Feeder
func (p Producer) Feed(
	rx <-chan consumer.Message,
	name string,
	ctx context.Context,
	fn consumer.TopicMapFn,
) error {
	if !p.active {
		return fmt.Errorf(
			"kafka async producer not active, cannot feed %s messages",
			name,
		)
	}
	if rx == nil {
		return fmt.Errorf(
			"missing channel, cannot feed async kafka producer with %s",
			name,
		)
	}
	if fn == nil {
		fn = func(consumer.Message) string {
			return "events"
		}
	}

	p.feeders.Add(1)
	go func(ctx context.Context) {
		debug := time.NewTicker(3 * time.Second)
		var count uint64
		defer p.feeders.Done()
	loop:
		for p.active {
			select {
			case msg, ok := <-rx:
				if !ok {
					break loop
				}
				p.handle.Input() <- &sarama.ProducerMessage{
					Timestamp: msg.Time,
					Key:       sarama.ByteEncoder(msg.Key),
					Value:     sarama.ByteEncoder(msg.Data),
					Topic:     fn(msg),
				}
				count++
			case <-debug.C:
				log.Infof("Sent %d events to kafka producer", count)
			case <-ctx.Done():
				break loop
			}
		}
	}(func() context.Context {
		if ctx == nil {
			return context.Background()
		}
		return ctx
	}())

	return nil
}

func (p Producer) Wait() {
	if p.feeders == nil {
		return
	}
	p.feeders.Wait()
}

func (p Producer) Close() error {
	if p.handle == nil {
		return fmt.Errorf("unable to close inactive kafka producer")
	}
	return p.handle.Close()
}

// Errors does not implement Error
// Only meant to allow producer errors to be checked externally
// TODO - return Error object with errors from async producer
func (p Producer) Errors() error {
	if p.errCount == 0 {
		return nil
	}
	return fmt.Errorf("kafka async producer has encountered %d errors", p.errCount)
}

func newProducerConfig() *sarama.Config {
	var config = sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.NoResponse
	config.Producer.Retry.Max = 5
	config.Producer.Compression = sarama.CompressionSnappy
	return config
}
