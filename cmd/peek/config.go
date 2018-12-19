package main

import (
	"os"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"

	"github.com/BurntSushi/toml"
	"github.com/ccdcoe/go-peek/outputs"
)

type mainConf struct {
	General       generalConf
	Kafka         kafkaConf
	ElasticSearch esConf
	EventTypes    map[string]mapTopics
}

type generalConf struct {
	Spooldir      string
	Workers       uint
	Errors        logConfig
	Notifications logConfig
}

type kafkaConf struct {
	Input, Output []string
	Topics        []string
	ConsumerGroup string
}

type esConf struct {
	Output    []string
	RenameMap *esRenameMapConf
	Inventory *esInventoryConf
}

type esRenameMapConf struct {
	Hosts       []string
	Index       string
	IPaddrIndex string
}

type esInventoryConf struct {
	Host, Index string
}

type saganConf struct {
	Brokers []string
	Topic   string
}

type mapTopics struct {
	Type  string
	Topic string
	Sagan *saganConf
}

type logConfig struct {
	Log    bool
	Sample int
}

func defaultConfg() *mainConf {
	return &mainConf{
		General: generalConf{
			Spooldir: "/var/spool/gopeek",
			Workers:  4,
			Errors: logConfig{
				Log:    true,
				Sample: -1,
			},
			Notifications: logConfig{
				Log:    true,
				Sample: -1,
			},
		},
		Kafka: kafkaConf{
			Input:         []string{"localhost:9092"},
			ConsumerGroup: "peek",
		},
		ElasticSearch: esConf{
			Output: []string{"http://localhost:9200"},
			RenameMap: &esRenameMapConf{
				Hosts:       []string{"http://localhost:9200"},
				Index:       "ladys",
				IPaddrIndex: "ipaddr-map",
			},
			Inventory: &esInventoryConf{
				Host:  "http://localhost",
				Index: "inventory-latest",
			},
		},
		EventTypes: map[string]mapTopics{},
	}
}

func (c mainConf) Print() error {
	encoder := toml.NewEncoder(os.Stdout)
	return encoder.Encode(c)
}

func (c mainConf) Topics() []string {
	var topics = make([]string, 0)
	for k, _ := range c.EventTypes {
		topics = append(topics, k)
	}
	return topics
}

func (c mainConf) GetTopicType(key string) string {
	return c.EventTypes[key].Type
}

func (c mainConf) MapEventTypes() map[string]string {
	var types = map[string]string{}
	for k, v := range c.EventTypes {
		types[k] = v.Type
	}
	return types
}

func (c mainConf) GetDestTopic(src string) string {
	return c.EventTypes[src].Topic
}

func (c mainConf) GetDestSaganTopic(src string) string {
	return c.EventTypes[src].Sagan.Topic
}

func (c mainConf) GetDestTimeElaIndex(timestamp time.Time, src string) string {
	return outputs.ElaIndex(c.EventTypes[src].Topic).Format(timestamp)
}

func consumerConfig() *cluster.Config {
	var config = cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	return config
}

func producerConfig() *sarama.Config {
	var config = sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.NoResponse
	config.Producer.Retry.Max = 5
	config.Producer.Compression = sarama.CompressionSnappy
	return config
}
