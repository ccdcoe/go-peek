package config

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

func NewConsumerConfig() *cluster.Config {
	var config = cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	return config
}

func NewProducerConfig() *sarama.Config {
	var config = sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.NoResponse
	config.Producer.Retry.Max = 5
	config.Producer.Compression = sarama.CompressionSnappy
	return config
}
