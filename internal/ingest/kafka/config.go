package kafka

import (
	cluster "github.com/bsm/sarama-cluster"
)

type KafkaConfig struct {
	Brokers       []string
	ConsumerGroup string
	Topics        []string
	SaramaConfig  *cluster.Config
}

func NewConsumerConfig() *cluster.Config {
	var config = cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	return config
}
