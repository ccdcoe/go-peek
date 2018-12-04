package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"runtime"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/ccdcoe/go-peek/decoder"
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
	Type       string
	Topic      string
	SaganTopic string
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

func (c mainConf) GetDestSaganTopic(src string) string {
	return c.EventTypes[src].SaganTopic
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
	// Multiplexer / Output start
	var wg sync.WaitGroup
	wg.Add(1)

	if err != nil {
		printErr(err)
		os.Exit(1)
	}

	go func(input chan decoder.DecodedMessage) {
		defer wg.Done()
		/*
			var (
				bulk = ela.Bulk()
			)
		*/
		var send = time.NewTicker(3 * time.Second)
		ela := NewBulk([]string{appConfg.ElasticSearch.Output})

	loop:
		for {
			select {
			case msg, ok := <-input:
				if !ok {
					break loop
				}
				// Main produce
				producer.Input() <- &sarama.ProducerMessage{
					Topic:     appConfg.GetDestTopic(msg.Topic),
					Value:     sarama.ByteEncoder(msg.Val),
					Key:       sarama.ByteEncoder(msg.Key),
					Timestamp: msg.Time,
				}

				// Sagan produce
				producer.Input() <- &sarama.ProducerMessage{
					Topic:     appConfg.GetDestSaganTopic(msg.Topic),
					Value:     sarama.StringEncoder(msg.Sagan),
					Key:       sarama.ByteEncoder(msg.Key),
					Timestamp: msg.Time,
				}

				idxName := fmt.Sprintf("%s-%s", msg.Topic, msg.Time.Format(esTimeFormat))
				ela.AddIndex(msg.Val, idxName)

			case <-send.C:
				ela.Flush()
			}
		}
	}(dec.Output)
	wg.Wait()

	if len(dec.Notifications) > 0 {
		fmt.Println(<-dec.Notifications)
	}

	fmt.Println("All done")
	fmt.Println(appConfg)
}

type ElaBulk struct {
	Data   [][]byte
	Hosts  []string
	Resps  chan *http.Response
	errors chan error
}

func NewBulk(hosts []string) *ElaBulk {
	return &ElaBulk{
		Hosts:  hosts,
		Data:   make([][]byte, 0),
		Resps:  make(chan *http.Response, 256),
		errors: make(chan error, 256),
	}
}

func (b ElaBulk) Errors() <-chan error {
	return b.errors
}

func (b *ElaBulk) AddIndex(item []byte, index string) *ElaBulk {
	meta, _ := json.Marshal(map[string]map[string]string{
		"index": {
			"_index": index,
			"_type":  "doc",
		},
	})
	b.Data = append(b.Data, meta)
	b.Data = append(b.Data, item)
	return b
}

func (b *ElaBulk) Flush() *ElaBulk {
	go func(data []byte) {
		buf := bytes.NewBuffer(data)
		buf.WriteRune('\n')
		resp, err := http.Post(b.Hosts[0]+"/_bulk", "application/x-ndjson", buf)
		if err != nil {
			if len(b.errors) == 256 {
				<-b.errors
			}
			b.errors <- err
		}
		if resp != nil {
			resp.Body.Close()
			if len(b.Resps) == 256 {
				<-b.Resps
			}
			b.Resps <- resp
		}
	}(append(bytes.Join(b.Data, []byte("\n"))))
	b.Data = make([][]byte, 0)
	return b
}

func printErr(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err.Error())
}
