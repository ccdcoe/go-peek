package kafka

import (
	"context"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/ccdcoe/go-peek/internal/ingest/message"
	"github.com/ccdcoe/go-peek/internal/logging"
)

type KafkaIngest struct {
	output        chan message.Message
	consumeCancel context.CancelFunc

	*cluster.Consumer
	logging.LogSender
}

func NewKafkaIngest(config *KafkaConfig) (*KafkaIngest, error) {
	var (
		err error
		ctx context.Context
		k   = &KafkaIngest{
			output: make(chan message.Message, 0),
		}
	)

	if err = config.Validate(); err != nil {
		return nil, err
	}
	k.LogSender = config.LogHandler

	if k.Consumer, err = cluster.NewConsumer(
		config.Brokers,
		config.ConsumerGroup,
		config.Topics,
		config.SaramaConfig,
	); err != nil {
		return nil, err
	}
	ctx, k.consumeCancel = context.WithCancel(context.Background())

	go func(ctx context.Context) {
	loop:
		for {
			select {
			case msg, ok := <-k.Consumer.Messages():
				if !ok {
					break loop
				}
				k.output <- message.Message{
					Data:   msg.Value,
					Offset: msg.Offset,
					Source: msg.Topic,
				}
			case <-ctx.Done():
				k.Consumer.Close()
			}
		}
	}(ctx)

	go func() {
		for not := range k.Consumer.Notifications() {
			k.LogSender.Notify(not)
		}
	}()
	go func() {
		for err := range k.Consumer.Errors() {
			k.LogSender.Error(err)
		}
	}()

	return k, nil
}

func (k KafkaIngest) Messages() <-chan message.Message {
	return k.output
}

func (k KafkaIngest) Halt() error {
	k.consumeCancel()
	return nil
}
