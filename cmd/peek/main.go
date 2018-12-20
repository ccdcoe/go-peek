package main

import (
	"flag"
	"fmt"
	"os"
	"path"
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
	fmt.Println(appConfg)

	// consumer start
	var (
		consumer *cluster.Consumer
		dec      *decoder.Decoder
		err      error
		errs     = make(chan error)
		wg       sync.WaitGroup
	)

	fmt.Println("starting consumer")
	if consumer, err = cluster.NewConsumer(
		appConfg.Kafka.Input,
		appConfg.Kafka.ConsumerGroup,
		appConfg.Topics(),
		consumerConfig(),
	); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	fmt.Println("Starting decoder")
	// decoder / worker start
	if dec, err = decoder.NewMessageDecoder(
		int(appConfg.General.Workers),
		consumer,
		appConfg.MapEventTypes(),
		appConfg.General.Spooldir,
		appConfg.ElasticSearch.Inventory.Host,
		appConfg.ElasticSearch.Inventory.Index,
	); err != nil {
		printErr(err)
		os.Exit(1)
	}

	// Producer start

	fmt.Println("starting main sarama producer")
	producer, err := sarama.NewAsyncProducer(appConfg.Kafka.Output, producerConfig())
	if err != nil {
		printErr(err)
	}

	if appConfg.General.Errors.Log {
		go logErrs(
			appConfg.General.Errors.Sample,
			consumer.Errors(),
			producer.Errors(),
			dec.Errors,
			errs,
		)
	}
	if appConfg.General.Notifications.Log {
		go logNotifications(
			appConfg.General.Notifications.Sample,
			dec.Notifications,
			consumer.Notifications(),
		)
	}

	// Multiplexer / Output start
	wg.Add(1)
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
			fmt.Fprintf(os.Stdout, "Starting channel consumer for %s\n", k)
			wg.Add(1)
			go saganProducer(
				k,
				saganChannels[k],
				&wg,
				appConfg,
				producerConfig(),
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
}

func printErr(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err.Error())
}
