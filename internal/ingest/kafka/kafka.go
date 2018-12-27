package kafka

import (
	cluster "github.com/bsm/sarama-cluster"
	"github.com/ccdcoe/go-peek/internal/ingest/message"
)

type KafkaIngest struct {
	Output chan message.Message

	*cluster.Consumer
}

func NewKafka(config KafkaConfig) (*KafkaIngest, error) {
	var err error
	if config.SaramaConfig == nil {
		config.SaramaConfig = NewConsumerConfig()
	}
	k := &KafkaIngest{
		Output: make(chan message.Message, 0),
	}
	if k.Consumer, err = cluster.NewConsumer(
		config.Brokers,
		config.ConsumerGroup,
		config.Topics,
		config.SaramaConfig,
	); err != nil {
		return nil, err
	}
	return k, nil
}

func (k KafkaIngest) Messages() <-chan message.Message {
	return k.Output
}
