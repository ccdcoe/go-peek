package kafka

import (
	"context"
)

type Config struct {
	Name          string
	Brokers       []string
	ConsumerGroup string
	Topics        []string
	Ctx           context.Context
	OffsetMode    OffsetMode
}

func NewDefaultConfig() *Config {
	return &Config{
		Brokers:       []string{"localhost:9092"},
		ConsumerGroup: "peek",
		Topics:        []string{},
		Ctx:           context.Background(),
	}
}

func (c *Config) Validate() error {
	if c == nil {
		c = NewDefaultConfig()
	}
	if c.Name == "" {
		c.Name = "default-consumer"
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
	return nil
}
