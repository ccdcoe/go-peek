package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/ccdcoe/go-peek/decoder"
	"github.com/olivere/elastic"
)

const esTimeFormat = "2006.01.02.15"

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
	Output string
}

type mapTopics struct {
	Type  string
	Topic string
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
			Output: "http://localhost:9200",
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

	// consumer start
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

	// decoder / worker start
	if dec, err = decoder.NewMessageDecoder(
		int(*workers),
		consumer,
		appConfg.MapEventTypes(),
		appConfg.General.Spooldir,
	); err != nil {
		printErr(err)
		os.Exit(1)
	}

	go func() {
		for err := range dec.Errors {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	go func() {
		for ntf := range dec.Notifications {
			log.Printf("Info: %s\n", ntf)
		}
	}()

	// Producer start
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.RequiredAcks = sarama.NoResponse
	producerConfig.Producer.Retry.Max = 5
	producerConfig.Producer.Compression = sarama.CompressionSnappy

	var errs = make(chan error, len(appConfg.EventTypes))

	go func() {
		for err := range errs {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	producer, err := sarama.NewAsyncProducer(appConfg.Kafka.Output, producerConfig)
	if err != nil {
		printErr(err)
	}
	go func() {
		for err := range producer.Errors() {
			errs <- fmt.Errorf("Failed to write msg: %s", err.Error())
		}
	}()

	ela, err := elastic.NewSimpleClient(
		elastic.SetURL(appConfg.ElasticSearch.Output),
	)
	if err != nil {
		printErr(err)
		os.Exit(1)
	}

	bulk := ela.Bulk()
	send := time.NewTicker(3 * time.Second)

loop:
	for {
		select {
		case msg, ok := <-dec.Output:
			if !ok {
				break loop
			}
			//fmt.Println(msg.Topic, appConfg.GetDestTopic(msg.Topic), string(msg.Val))
			producer.Input() <- &sarama.ProducerMessage{
				Topic:     appConfg.GetDestTopic(msg.Topic),
				Value:     sarama.ByteEncoder(msg.Val),
				Key:       sarama.ByteEncoder(msg.Key),
				Timestamp: msg.Time,
			}

			//fmt.Println(msg.Time.Format(esTimeFormat))
			idxName := fmt.Sprintf("%s-%s", msg.Topic, msg.Time.Format(esTimeFormat))

			idx := elastic.NewBulkIndexRequest().Index(idxName).Type("doc").Doc(msg.Val)
			bulk.Add(idx)
		case <-send.C:
			if _, err := bulk.Do(context.TODO()); err != nil {
				fmt.Println(err.Error())
			}

		}
	}

	if len(dec.Notifications) > 0 {
		fmt.Println(<-dec.Notifications)
	}

	fmt.Println("All done")
	fmt.Println(appConfg)
}

func printErr(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err.Error())
}
