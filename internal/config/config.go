package config

import (
	"io"
	"sync"
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
			Output: []string{"http://localhost:9200"},
			RenameMap: &ElaRenameMapConfig{
				Hosts:     []string{"http://localhost:9200"},
				HostIndex: "ladys",
				AddrIndex: "ipaddr-map",
			},
			Inventory: &ElaInventoryConfig{
				Hosts:      []string{"http://localhost:9200"},
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
		c.Stream[name] = *NewDefaultStreamConfig(name)
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
			Hosts: c.Elastic.Inventory.Hosts,
			Index: c.Elastic.Inventory.GrainIndex,
		}
	}
	return conf
}

func (c Config) OutputConfig(logs logging.LogHandler) *outputs.OutputConfig {
	var (
		brokers    = []string{"localhost:9092"}
		feedback   = []string{"localhost:9092"}
		elaproxies = []string{"http://localhost:9200"}
		topicmap   = map[string]outputs.OutputTopicConfig{}
	)

	if c.Kafka != nil {
		if c.Kafka.Output != nil && len(c.Kafka.Output) > 0 {
			brokers = c.Kafka.Output
		} else if c.Kafka.Input != nil && len(c.Kafka.Input) > 0 {
			brokers = c.Kafka.Input
		}
		if c.Kafka.Input != nil && len(c.Kafka.Input) > 0 {
			feedback = c.Kafka.Input
		}
	}

	if c.Stream != nil && len(c.Stream) > 0 {
		topicmap = c.Stream.GetOutputConfigMap()
	}

	if c.Elastic != nil {
		if c.Elastic.Output != nil && len(c.Elastic.Output) > 0 {
			elaproxies = c.Elastic.Output
		}
	}

	return &outputs.OutputConfig{
		MainKafkaBrokers:     brokers,
		FeedbackKafkaBrokers: feedback,
		TopicMap:             topicmap,
		KeepKafkaTopic:       false,
		ElaProxies:           elaproxies,
		// *TODO* add to config
		ElaFlush: 3 * time.Second,
		Wait:     &sync.WaitGroup{},
		Logger:   logs,
	}
}

func (c Config) GetReplayStatConfig(
	logger logging.LogHandler,
	from, to time.Time,
	timeout time.Duration,
) []decoder.SourceStatConfig {
	configs := []decoder.SourceStatConfig{}
	if logger == nil {
		logger = logging.NewLogHandler()
	}
	for _, v := range c.Stream {
		conf := &decoder.SourceStatConfig{
			Name:   v.Type,
			Source: v.Dir,
			LogReplayWorkerConfig: decoder.LogReplayWorkerConfig{
				Workers: int(c.General.Workers),
				Logger:  logger,
				From:    from,
				To:      to,
				Timeout: timeout,
			},
		}
		configs = append(configs, *conf)
	}
	return configs
}
