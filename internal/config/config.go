package config

import (
	"fmt"
	"io"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/ccdcoe/go-peek/internal/decoder"
	"github.com/ccdcoe/go-peek/internal/ingest/kafka"
	"github.com/ccdcoe/go-peek/internal/logging"
	"github.com/ccdcoe/go-peek/internal/outputs"
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
	return &decoder.DecoderConfig{
		Input:        input,
		Spooldir:     c.General.Spooldir,
		EventMap:     c.EventTypes(),
		Workers:      int(c.General.Workers),
		IgnoreSigInt: false,
		LogHandler:   logs,
	}
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

type ElaConfig struct {
	// List of elasticsearch proxy hosts in http://addr:port format
	// Genaral config for outputting messages into timestamped indices
	Output []string

	// For storing anonymized names to actual name and ip to common name mappings
	// Separate from main output config, as this mapping should not be exposed to players
	// Needed by other tools
	RenameMap *ElaRenameMapConfig

	// Inventory that contains full information about hosts and targets
	// Separate, as info can be used by green team as well
	Inventory *ElaInventoryConfig
}

type ElaRenameMapConfig struct {
	// List of elasticsearch proxy hosts in http://addr:port format
	Hosts []string

	// Indices for storing host to pretty name and known ip to pretty name mappings
	HostIndex string
	AddrIndex string
}

type ElaInventoryConfig struct {
	// List of elasticsearch proxy hosts in http://addr:port format
	Hosts []string

	// Index containing inventory from salt grains
	GrainIndex string
}

type StreamConfig struct {
	// Name of kafka input topic
	// Redundant if object is kept in map
	Name string

	// Type of messages in stream
	// To separate event.Event objects
	Type string

	// Output topic, elastic index name, etc
	// Timestamp should be appended to elastic index programmatically
	Topic string

	// Sagan is "Suricata but for logs"
	// Uses Suricata engine to correlate logs
	// Expects a specific log format
	// This config feeds messages back into backend/middleware kafka cluster in that format
	// So, new eventstream could be generated from correlations
	Sagan *SaganConfig
}

func NewStreamConfig() *StreamConfig {
	return &StreamConfig{
		Sagan: &SaganConfig{},
	}
}

func NewDefaultStreamConfig(stream string) *StreamConfig {
	var s = NewStreamConfig()
	s.Name = stream
	s.Type = stream
	s.Topic = stream
	s.Sagan = NewDefaultSaganConfig(stream)
	return s
}

func (s StreamConfig) ElaIdx(timestamp time.Time) string {
	return outputs.ElaIndex(s.Topic).Format(timestamp)
}

type Streams map[string]*StreamConfig

func (s Streams) EventTypes() map[string]string {
	var types = make(map[string]string)
	for k, v := range s {
		types[k] = v.Type
	}
	return types
}

func (s Streams) GetType(key string) (string, bool) {
	if val, ok := s[key]; ok {
		return val.Type, true
	}
	return "", false
}
func (s Streams) GetTopic(key string) (string, bool) {
	if val, ok := s[key]; ok {
		return val.Topic, true
	}
	return "", false
}
func (s Streams) GetSaganTopic(key string) (string, bool) {
	if val, ok := s[key]; ok {
		if val.Sagan == nil {
			return fmt.Sprintf("%s-sagan", val.Topic), false
		}
		return val.Sagan.Topic, true
	}
	return "", false
}

func (s Streams) Topics() []string {
	var topics = make([]string, 0)
	for k := range s {
		topics = append(topics, k)
	}
	return topics
}

func (s Streams) ElaIdx(timestamp time.Time, event string) string {
	if val, ok := s[event]; ok {
		return val.ElaIdx(timestamp)
	}
	return outputs.ElaIndex("events").Format(timestamp)
}

type SaganConfig struct {
	// Kafka brokers list
	Brokers []string

	// Topic name for sagan, should be separate from original stream
	// Do not cross the streams
	Topic string
}

func NewSaganConfig() *SaganConfig {
	return &SaganConfig{
		Brokers: []string{"localhost:9092"},
	}
}

func NewDefaultSaganConfig(stream string) *SaganConfig {
	var s = NewSaganConfig()
	s.Topic = stream
	return s
}
