package kafka

import (
	"context"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
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
