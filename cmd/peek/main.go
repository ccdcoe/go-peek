package main

import (
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/BurntSushi/toml"
	"github.com/ccdcoe/go-peek/internal/config"
	"github.com/ccdcoe/go-peek/internal/ingest"
	"github.com/ccdcoe/go-peek/internal/ingest/kafka"
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

	if *exampleConf {
		config.NewExampleConfig().Toml(os.Stdout)
		os.Exit(1)
	}

	var (
		err      error
		appConfg = config.NewDefaultConfig()
	)

	fmt.Fprintf(os.Stdout, "Loading main config\n")
	if _, err = toml.DecodeFile(*confPath, &appConfg); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	if appConfg.DefaultStreams() {
		fmt.Fprintf(os.Stdout, "No streams configured, using default\n")
	}

	var (
		consumer ingest.Ingester
	)

	fmt.Fprintf(os.Stdout, "Starting consumer\n")
	if consumer, err = kafka.NewKafkaIngest(
		appConfg.KafkaConfig(),
	); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	// *TODO* temp code during devel
	fmt.Fprintf(os.Stdout, "Processing messages\n")
	for msg := range consumer.Messages() {
		fmt.Println(string(msg.Data))
	}
	/*
		// consumer start
		var (
			consumer *cluster.Consumer
			producer sarama.AsyncProducer
			dec      *decoder.Decoder
			err      error
			errs     = make(chan error)
			wg       sync.WaitGroup
		)

		fmt.Println("starting consumer")
		if consumer, err = cluster.NewConsumer(
			appConfg.Kafka.Input,
			appConfg.Kafka.ConsumerGroup,
			appConfg.Stream.Topics(),
			config.NewConsumerConfig(),
		); err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(1)
		}

		fmt.Println("Starting decoder")
		// decoder / worker start
		if dec, err = decoder.NewMessageDecoder(
			int(appConfg.General.Workers),
			consumer,
			appConfg.Stream.EventTypes(),
			appConfg.General.Spooldir,
			appConfg.Elastic.Inventory.Hosts[0],
			appConfg.Elastic.Inventory.GrainIndex,
		); err != nil {
			printErr(err)
			os.Exit(1)
		}

		// Producer start
		fmt.Println("starting main sarama producer")
		if producer, err = sarama.NewAsyncProducer(
			appConfg.Kafka.Output,
			config.NewProducerConfig(),
		); err != nil {
			printErr(err)
		}

		if appConfg.Errors.Enable {
			go logErrs(
				appConfg.Errors.Sample,
				consumer.Errors(),
				producer.Errors(),
				dec.Errors,
				errs,
			)
		}
		if appConfg.Notifications.Enable {
			go logNotifications(
				appConfg.Notifications.Sample,
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
		for k, v := range appConfg.Stream {
			if v.Sagan != nil {
				fmt.Fprintf(os.Stdout, "Making channel sagan for %s to %s topic %s\n", k,
					strings.Join(appConfg.Stream[k].Sagan.Brokers, ","),
					appConfg.Stream[k].Sagan.Topic)
				saganChannels[k] = make(chan decoder.DecodedMessage)
			}
		}

		// sagan kafka topics send
		for k, v := range appConfg.Stream {
			if v.Sagan != nil {
				fmt.Fprintf(os.Stdout, "Starting channel consumer for %s\n", k)
				wg.Add(1)
				go saganProducer(
					k,
					saganChannels[k],
					&wg,
					appConfg,
					config.NewProducerConfig(),
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
	*/
	fmt.Println("All done")
}

func printErr(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err.Error())
}
