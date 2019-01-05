package config

import (
	"io"

	"github.com/BurntSushi/toml"
	"github.com/ccdcoe/go-peek/internal/decoder"
	"github.com/ccdcoe/go-peek/internal/ingest/kafka"
	"github.com/ccdcoe/go-peek/internal/logging"
	"github.com/ccdcoe/go-peek/internal/types"
)

type Config struct {
	General *GeneralConfig

	Errors        *LogConfig
	Notifications *LogConfig

	Kafka   *KafkaConfig
	Elastic *ElaConfig

	Stream Streams
}

func NewDefaultConfig() *Config {
	return &Config{
		General: &GeneralConfig{
			Spooldir: "/var/log/gopeek",
			Workers:  4,
		},
		Errors: &LogConfig{
			Enable: true,
			Sample: -1,
		},
		Notifications: &LogConfig{
			Enable: true,
			Sample: -1,
		},
		Kafka: &KafkaConfig{
			Input:         []string{"localhost:9092"},
			Output:        []string{"localhost:9093"},
			ConsumerGroup: "peek",
		},
		Elastic: &ElaConfig{
			Output: []string{"localhost:9200"},
			RenameMap: &ElaRenameMapConfig{
				Hosts:     []string{"localhost:9200"},
				HostIndex: "ladys",
				AddrIndex: "ipaddr-map",
			},
			Inventory: &ElaInventoryConfig{
				Hosts:      []string{"localhost:9200"},
				GrainIndex: "inventory-latest",
			},
		},
		Stream: make(Streams),
	}
}

func NewExampleConfig() *Config {
	var c = NewDefaultConfig()
	c.DefaultStreams()
	return c
}

func (c *Config) DefaultStreams() bool {
	if c.Stream == nil || len(c.Stream) == 0 {
		var name = "syslog"
		c.Stream[name] = NewDefaultStreamConfig(name)
		return true
	}
	return false
}

func (c Config) Toml(dest io.Writer) error {
	encoder := toml.NewEncoder(dest)
	return encoder.Encode(c)
}

func (c Config) EventTypes() map[string]string {
	var types = map[string]string{}
	for _, val := range c.Stream {
		types[val.Name] = val.Type
	}
	return types
}

func (c Config) KafkaConfig() *kafka.KafkaConfig {
	return &kafka.KafkaConfig{
		Brokers:       c.Kafka.Input,
		ConsumerGroup: c.Kafka.ConsumerGroup,
		Topics:        c.Stream.Topics(),
	}
}

func (c Config) DecoderConfig(input types.Messager, logs logging.LogHandler) *decoder.DecoderConfig {
	var conf = &decoder.DecoderConfig{
		Input:        input,
		Spooldir:     c.General.Spooldir,
		EventMap:     c.EventTypes(),
		Workers:      int(c.General.Workers),
		IgnoreSigInt: false,
		LogHandler:   logs,
	}
	if c.Elastic != nil && c.Elastic.Inventory != nil {
		conf.InventoryConfig = &types.ElaTargetInventoryConfig{
			c.Elastic.Inventory.Hosts,
			c.Elastic.Inventory.GrainIndex,
		}
	}
	return conf
}

type GeneralConfig struct {
	// Working directory where persistence files are being kept
	Spooldir string

	// Number of async goroutines for decoding messages
	// All workers shall consume from and output to the same channel
	// Uint, so we don't need to check for negative numbers
	Workers uint
}

type LogConfig struct {
	// Enable or disable logging
	Enable bool

	// Log every n-th messages
	// Useful for things such as parse errors in high-throughput message queues
	Sample int
}

type KafkaConfig struct {
	// Allows messages to be consumed from one cluster and produced to another
	// Due to our setup where we have separate clustes for middleware and exposing to frontend
	Input  []string
	Output []string

	// ConsumerGroup used for committing offsets
	ConsumerGroup string
}
