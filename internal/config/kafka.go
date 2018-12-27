package config

import (
	"github.com/Shopify/sarama"
)

func NewProducerConfig() *sarama.Config {
	var config = sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.NoResponse
	config.Producer.Retry.Max = 5
	config.Producer.Compression = sarama.CompressionSnappy
	return config
}
