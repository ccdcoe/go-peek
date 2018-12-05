package main

import (
	"os"
	"time"

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
	Spooldir string
}

type kafkaConf struct {
	Input, Output []string
	Topics        []string
	ConsumerGroup string
}

type esConf struct {
	Output       []string
	NameMapDump  []string
	NameMapIndex string
}

type saganConf struct {
	Brokers []string
	Topic   string
}

type mapTopics struct {
	Type         string
	Topic        string
	ElasticIndex outputs.ElaIndex
	Sagan        *saganConf
}

func defaultConfg() *mainConf {
	return &mainConf{
		General: generalConf{
			Spooldir: "/var/spool/gopeek",
		},
		Kafka: kafkaConf{
			Input:         []string{"localhost:9092"},
			ConsumerGroup: "peek",
		},
		ElasticSearch: esConf{
			Output: []string{"http://localhost:9200"},
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

func (c mainConf) GetDestElaIndex(src string) string {
	return c.EventTypes[src].ElasticIndex.String()
}

func (c mainConf) GetDestTimeElaIndex(timestamp time.Time, src string) string {
	return c.EventTypes[src].ElasticIndex.Format(timestamp)
}
