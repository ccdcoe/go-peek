package kafka

import (
	"fmt"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/ccdcoe/go-peek/internal/logging"
)

type KafkaConfig struct {
	Brokers       []string
	ConsumerGroup string
	Topics        []string
	SaramaConfig  *cluster.Config

	logging.LogHandler
}

func (c *KafkaConfig) Validate() error {
	if c.LogHandler == nil {
		c.LogHandler = logging.NewLogHandler()
	}
	if c.SaramaConfig == nil {
		c.SaramaConfig = NewConsumerConfig()
	}
	if c.Brokers == nil || len(c.Brokers) == 0 {
		return fmt.Errorf("Broker config missing")
	}
	if c.Topics == nil || len(c.Topics) == 0 {
		return fmt.Errorf("Topic config missing")
	}
	return nil
}

func NewConsumerConfig() *cluster.Config {
	var config = cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	return config
}
