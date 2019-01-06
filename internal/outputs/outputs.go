package outputs

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ccdcoe/go-peek/internal/logging"
	"github.com/ccdcoe/go-peek/internal/types"
)

const defaultTopic = "events"

func NewProducerConfig() *sarama.Config {
	var config = sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.NoResponse
	config.Producer.Retry.Max = 5
	config.Producer.Compression = sarama.CompressionSnappy
	return config
}

type Output <-chan types.Message

func (o Output) Produce(
	config OutputConfig,
	ctx context.Context,
) (logging.LogSender, error) {
	var (
		err      error
		brokers  []string
		producer sarama.AsyncProducer
		feedback sarama.AsyncProducer
		logger   logging.LogHandler
		flush    time.Duration
	)

	if config.MainKafkaBrokers == nil || len(config.MainKafkaBrokers) == 0 {
		return nil, fmt.Errorf("Kafka producer brokers missing")
	}

	if (config.MainTopicMap == nil || len(config.MainTopicMap) == 0) && !config.KeepKafkaTopic {
		return nil, fmt.Errorf("Kafka topic map missing or empty. Please provide or enable KeepKafkaTopic")
	}

	brokers = config.MainKafkaBrokers
	if producer, err = sarama.NewAsyncProducer(
		brokers,
		NewProducerConfig(),
	); err != nil {
		return nil, err
	}

	if config.ElaProxies == nil {
		return nil, fmt.Errorf("Elastic proxy config missing")
	}

	if config.ElaFlush < 1*time.Second {
		flush = 1 * time.Second
	} else {
		flush = config.ElaFlush
	}

	var (
		send = time.NewTicker(flush)
		ela  = NewBulk(config.ElaProxies, logger)
	)

	if config.Logger != nil {
		logger = config.Logger
	} else {
		logger = logging.NewLogHandler()
	}

	if config.FeedbackKafkaBrokers == nil || len(config.FeedbackKafkaBrokers) == 0 {
		logger.Notify("Feedback brokers not configured, defaulting to main")
		brokers = config.MainKafkaBrokers
	} else {
		brokers = config.FeedbackKafkaBrokers
	}

	// *TODO* map[string]AsyncProducer if more formats or clusters are needed
	if feedback, err = sarama.NewAsyncProducer(
		brokers,
		NewProducerConfig(),
	); err != nil {
		return nil, err
	}

	if config.Wait != nil {
		config.Wait.Add(1)
	}
	go func(
		wg *sync.WaitGroup,
		mainmap,
		fbmap map[string]string,
		keepKafkaTopic bool,
	) {
		defer producer.Close()
		defer feedback.Close()
		defer wg.Done()
		defer ela.Flush()

		var topic string
	loop:
		for {
			select {
			case msg, ok := <-o:
				if !ok {
					break loop
				}

				if keepKafkaTopic {
					topic = msg.Source
				} else if val, ok := mainmap[msg.Source]; ok {
					topic = val
				} else {
					topic = defaultTopic
				}

				producer.Input() <- &sarama.ProducerMessage{
					Timestamp: msg.Time,
					Key:       sarama.ByteEncoder(msg.Key),
					Value:     sarama.ByteEncoder(msg.Data),
					Topic:     topic,
				}
				ela.AddIndex(msg.Data, defaultTopic)

				// Output for sagan
				if fbmap != nil {
					var format string
					if msg.Formats == nil {
						// *TODO* return custom error type with full event.Event for debug
						logger.Error(fmt.Errorf("Custom format requested but string missing"))
						continue loop
					}
					if val, ok := msg.Formats["sagan"]; !ok {
						logger.Error(fmt.Errorf("Sagan format requested but string missing"))
					} else {
						format = val
					}
					if val, ok := fbmap[msg.Source]; ok {
						topic = val
					} else {
						topic = strings.Join([]string{topic, "sagan"}, "-")
					}
					feedback.Input() <- &sarama.ProducerMessage{
						Timestamp: msg.Time,
						Key:       sarama.ByteEncoder(msg.Key),
						Value:     sarama.StringEncoder(format),
						Topic:     topic,
					}
				}
			case <-ctx.Done():
				break loop
			case <-send.C:
				ela.Flush()
			}
		}
	}(
		config.Wait,
		config.MainTopicMap,
		config.FeedbackTopicMap,
		config.KeepKafkaTopic,
	)
	return logger, nil
}
