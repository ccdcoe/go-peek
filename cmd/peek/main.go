package main

import (
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
