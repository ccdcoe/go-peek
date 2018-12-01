package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"

	"github.com/BurntSushi/toml"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/ccdcoe/go-peek/decoder"
)

var (
	mainFlags = flag.NewFlagSet("main", flag.ExitOnError)
	confPath  = mainFlags.String("config", path.Join(
		os.Getenv("GOPATH"), "etc", "peek.toml"),
		`Configuration file`)
	workers = mainFlags.Uint("workers", uint(runtime.NumCPU()),
		`Worker count. Defaults to CPU thread count.`)
	exampleConf = mainFlags.Bool("example-config", false,
		`Print default TOML config and exit`)
)

type mainConf struct {
	Kafka      kafkaConf
	EventTypes map[string]mapTopics
}

type kafkaConf struct {
	Input, Output []string
	Topics        []string
	ConsumerGroup string
}

type mapTopics struct {
	Type string
}

func defaultConfg() *mainConf {
	return &mainConf{
		Kafka: kafkaConf{
			Input:         []string{"localhost:9092"},
			Output:        []string{"localhost:9093"},
			ConsumerGroup: "peek",
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

// Kafka handler

func main() {
	mainFlags.Parse(os.Args[1:])
	appConfg := defaultConfg()

	if *exampleConf {
		appConfg.Print()
		os.Exit(1)
	}

	if _, err := toml.DecodeFile(*confPath, &appConfg); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}
	fmt.Println(appConfg.Topics())

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	var (
		consumer *cluster.Consumer
		dec      *decoder.Decoder
		err      error
	)

	if consumer, err = cluster.NewConsumer(
		appConfg.Kafka.Input,
		appConfg.Kafka.ConsumerGroup,
		appConfg.Topics(),
		config,
	); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	go func() {
		for ntf := range consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}()

	if dec, err = decoder.NewMessageDecoder(
		int(*workers),
		consumer,
		appConfg.MapEventTypes(),
	); err != nil {
		printErr(err)
		os.Exit(1)
	}

	go func() {
		for err := range dec.Errors {
			printErr(err)
		}
	}()

	for msg := range dec.Output {
		fmt.Println(msg.Source())
	}
}

func printErr(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err.Error())
}

func jsonUnmarshalErrHandle(msg []byte, target interface{}) {
	if err := json.Unmarshal(msg, &target); err != nil {
		printErr(err)
	}
}
