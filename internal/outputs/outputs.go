package outputs

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ccdcoe/go-peek/internal/logging"
	"github.com/ccdcoe/go-peek/internal/types"
)

func NewProducerConfig() *sarama.Config {
	var config = sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.NoResponse
	config.Producer.Retry.Max = 5
	config.Producer.Compression = sarama.CompressionSnappy
	return config
}

type OutputConfig struct {
	KafkaBrokers []string

	ElaProxies []string
	ElaFlush   time.Duration

	Logger logging.LogHandler
}

type Output <-chan types.Message

func (o Output) Produce(
	config OutputConfig,
	ctx context.Context,
) (logging.LogSender, error) {
	if config.KafkaBrokers == nil {
		return nil, fmt.Errorf("Kafka producer brokers missing")
	}
	if config.ElaProxies == nil {
		return nil, fmt.Errorf("Elastic proxy config missing")
	}

	var (
		err      error
		producer sarama.AsyncProducer
		logger   logging.LogHandler
		flush    time.Duration
	)

	if config.ElaFlush < 1*time.Second {
		flush = 1 * time.Second
	} else {
		flush = config.ElaFlush
	}

	if config.Logger != nil {
		logger = config.Logger
	} else {
		logger = logging.NewLogHandler()
	}
	var (
		send = time.NewTicker(flush)
		ela  = NewBulk(config.ElaProxies, logger)
	)

	if producer, err = sarama.NewAsyncProducer(
		config.KafkaBrokers,
		NewProducerConfig(),
	); err != nil {
		return nil, err
	}

	go func() {
		defer producer.Close()
	loop:
		for {
			select {
			case _, ok := <-o:
				if !ok {
					break loop
				}
			case <-ctx.Done():
				break loop
			case <-send.C:
				ela.Flush()
			}
		}
	}()

	return logger, nil
}
