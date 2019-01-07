package outputs

import (
	"context"
	"fmt"
	"strings"
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

	if (config.TopicMap == nil || len(config.TopicMap) == 0) && !config.KeepKafkaTopic {
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
		logger.Notify(fmt.Sprintf(
			"Feedback brokers configured, using %s",
			strings.Join(config.FeedbackKafkaBrokers, ","),
		))
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
	go func(config OutputConfig) {
		defer producer.Close()
		defer feedback.Close()
		defer config.Wait.Done()
		defer ela.Flush()

		var topic string
		var saganset = config.SaganSet()
	loop:
		for {
			select {
			case msg, ok := <-o:
				if !ok {
					break loop
				}

				if config.KeepKafkaTopic {
					topic = strings.Replace(msg.Source, "/", "-", -1)
				} else if val, ok := config.TopicMap[msg.Source]; ok {
					topic = val.Topic
				} else {
					//logger.Error(fmt.Errorf())
					topic = defaultTopic
				}

				producer.Input() <- &sarama.ProducerMessage{
					Timestamp: msg.Time,
					Key:       sarama.ByteEncoder(msg.Key),
					Value:     sarama.ByteEncoder(msg.Data),
					Topic:     topic,
				}
				ela.AddIndex(msg.Data, defaultTopic)

				// *TODO* Move to function
				// *TODO* Support multiple distinct formats
				if saganset[msg.Source] {
					var (
						format    string
						formatKey = "sagan"
					)

					topic = topic + "-" + formatKey
					if msg.Formats == nil {
						// *TODO* return custom error type with full event.Event for debug
						logger.Error(fmt.Errorf(
							"%s format requested for source %s output %s but format map missing",
							formatKey,
							msg.Source,
							topic,
						))
						continue loop
					}
					if val, ok := msg.Formats[formatKey]; !ok {
						logger.Error(fmt.Errorf(
							"%s format requested for source %s output %s but value missing",
							formatKey,
							msg.Source,
							topic,
						))
						continue loop
					} else {
						format = val
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
	}(config)
	return logger, nil
}
