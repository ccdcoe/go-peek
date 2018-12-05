package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/ccdcoe/go-peek/decoder"
	"github.com/ccdcoe/go-peek/outputs"
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
	ElasticIndex elaIndex
	Sagan        *saganConf
}

type elaIndex string

func (e elaIndex) Format(timestamp time.Time) string {
	return strings.Join([]string{e.String(), timestamp.Format(e.Hourly())}, "-")
}
func (e elaIndex) Hourly() string { return "2006.01.02.15" }
func (e elaIndex) Daily() string  { return "2006.01.02" }
func (e elaIndex) String() string { return string(e) }

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

// Kafka handler

func main() {
	mainFlags.Parse(os.Args[1:])
	appConfg := defaultConfg()

	if *exampleConf {
		appConfg.Print()
		os.Exit(1)
	}

	fmt.Println("Loading config")
	if _, err := toml.DecodeFile(*confPath, &appConfg); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}
	//fmt.Println(appConfg.Topics())
	fmt.Println(appConfg)

	// consumer start
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	var (
		consumer *cluster.Consumer
		dec      *decoder.Decoder
		err      error
	)

	fmt.Println("starting consumer")
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

	fmt.Println("Starting decoder")
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

	fmt.Println(len(appConfg.EventTypes))

	var errs = make(chan error)

	go func() {
		for err := range errs {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	fmt.Println("starting main sarama producer")
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

	go dumpNames(
		time.NewTicker(5*time.Second),
		appConfg,
		dec,
		errs,
	)

	fmt.Println("starting channels for sagan producer messages")
	var saganChannels = make(map[string]chan decoder.DecodedMessage)
	for k, v := range appConfg.EventTypes {
		if v.Sagan != nil {
			fmt.Fprintf(os.Stdout, "Making channel sagan for %s to %s topic %s\n", k,
				strings.Join(appConfg.EventTypes[k].Sagan.Brokers, ","),
				appConfg.EventTypes[k].Sagan.Topic)
			saganChannels[k] = make(chan decoder.DecodedMessage)
		}
	}

	// sagan kafka topics send
	for k, v := range appConfg.EventTypes {
		if v.Sagan != nil {
			wg.Add(1)
			fmt.Fprintf(os.Stdout, "Starting channel consumer for %s\n", k)
			go saganProducer(
				k,
				saganChannels[k],
				&wg,
				appConfg,
				producerConfig,
				errs,
			)
		}
	}
	if len(errs) > 0 {
		printErr(<-errs)
		os.Exit(1)
	}

	fmt.Println("Starting main decoded message consumer")
	go decodedMessageConsumer(
		dec.Output,
		&wg,
		appConfg,
		saganChannels,
		producer,
	)
	wg.Wait()

	if len(dec.Notifications) > 0 {
		fmt.Println(<-dec.Notifications)
	}

	fmt.Println("All done")
	fmt.Println(appConfg)
}

func printErr(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err.Error())
}

func decodedMessageConsumer(
	input chan decoder.DecodedMessage,
	wg *sync.WaitGroup,
	appConfg *mainConf,
	saganChannels map[string]chan decoder.DecodedMessage,
	producer sarama.AsyncProducer,
) {
	defer wg.Done()

	var (
		send = time.NewTicker(3 * time.Second)
		ela  = outputs.NewBulk(appConfg.ElasticSearch.Output)
	)

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

			if _, ok := saganChannels[msg.Topic]; ok {
				saganChannels[msg.Topic] <- msg
			}
			ela.AddIndex(msg.Val, appConfg.GetDestTimeElaIndex(msg.Time, msg.Topic))

		case <-send.C:
			ela.Flush()
		}
	}
	fmt.Println("Message consumer done")
	for k := range saganChannels {
		close(saganChannels[k])
	}
}

func saganProducer(
	id string,
	input chan decoder.DecodedMessage,
	wg *sync.WaitGroup,
	appConfg *mainConf,
	producerConfig *sarama.Config,
	errs chan error,
) {
	defer wg.Done()
	defer fmt.Fprintf(os.Stdout, "Sagan consumer %s done\n", id)

	var topic = appConfg.EventTypes[id].Sagan.Topic
	saganProducer, err := sarama.NewAsyncProducer(
		appConfg.EventTypes[id].Sagan.Brokers,
		producerConfig)

	if err != nil {
		printErr(err)
		errs <- err
	}
	defer saganProducer.Close()

loop:
	for {
		select {
		case msg, ok := <-input:
			if !ok {
				break loop
			}
			// Sagan produce
			saganProducer.Input() <- &sarama.ProducerMessage{
				Topic:     topic,
				Value:     sarama.StringEncoder(msg.Sagan),
				Key:       sarama.ByteEncoder(msg.Key),
				Timestamp: msg.Time,
			}
		}
	}
}

func dumpNames(
	dumpNames *time.Ticker,
	appConfg *mainConf,
	dec *decoder.Decoder,
	errs chan error,
) {
	defer dumpNames.Stop()
	fmt.Println("starting elastic name dumper")
	elaNameDumper := outputs.NewBulk(appConfg.ElasticSearch.NameMapDump)
loop:
	for {
		select {
		case _, ok := <-dumpNames.C:
			if !ok {
				break loop
			}
			for k, v := range dec.Names() {
				data, _ := json.Marshal(struct {
					Original, Pretty string
				}{
					Original: k,
					Pretty:   v,
				})
				elaNameDumper.AddIndex(data, appConfg.ElasticSearch.NameMapIndex, k)
			}
			elaNameDumper.Flush()
		}
	}
	fmt.Println("elastic name dumper done")
}
