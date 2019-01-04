package main

import (
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/BurntSushi/toml"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/ccdcoe/go-peek/internal/config"
	"github.com/ccdcoe/go-peek/internal/decoder"
	"github.com/ccdcoe/go-peek/internal/ingest/kafka"
	"github.com/ccdcoe/go-peek/internal/logging"
	"github.com/ccdcoe/go-peek/internal/types"
)

var (
	mainFlags = flag.NewFlagSet("main", flag.ExitOnError)
	confPath  = mainFlags.String("config", path.Join(
		os.Getenv("GOPATH"), "etc", "peek.toml"),
		`Configuration file`)
)

var usageStr = `
Usage: peek [options] [command] [args]

Commands:

	online					Process online kafka streams, default option
	consume					Read messages from configured kafka topics and print messages to stdout
	replay					Replay log files to simulate kafka stream
	example-config				Print example toml config and exit
	help					Print commands and exit

Options:
`

func usage() {
	fmt.Fprintln(os.Stderr, usageStr)
	mainFlags.PrintDefaults()
	os.Exit(1)
}

func exampleConfig() {
	config.NewExampleConfig().Toml(os.Stdout)
	os.Exit(1)
}

func main() {
	mainFlags.Parse(os.Args[1:])

	var (
		command  string
		args     []string
		commandF func(args []string, conf *config.Config) error
		err      error
		appConfg = config.NewDefaultConfig()
	)

	args = mainFlags.Args()
	if len(args) == 0 {
		fmt.Fprintf(os.Stdout, "No command set, assuming online\n")
		command = "online"
	} else {
		command = args[0]
		args = args[1:]
	}

	switch command {
	case "online":
		commandF = doOnlineProcess
	case "consume":
		commandF = doOnlineConsume
	case "replay":
		commandF = doReplay
	case "example-config":
		exampleConfig()
	case "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command, %s\n", command)
		usage()
	}

	fmt.Fprintf(os.Stdout, "Loading main config\n")
	if _, err = toml.DecodeFile(*confPath, &appConfg); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(2)
	}

	if appConfg.DefaultStreams() {
		fmt.Fprintf(os.Stdout, "No streams configured, using default\n")
	}

	fmt.Fprintf(os.Stdout, "Running main command\n")
	if err = commandF(args, appConfg); err != nil {
		// *TODO* error type switch here
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(2)
	}
	fmt.Fprintf(os.Stdout, "Done\n")
}

func doHelp(args []string, appConfg *config.Config) error {
	usage()
	return nil
}

func doOnlineConsume(args []string, appConfg *config.Config) error {
	var (
		consumer types.Messager
		err      error
	)

	fmt.Fprintf(os.Stdout, "Starting consumer\n")
	if consumer, err = kafka.NewKafkaIngest(
		appConfg.KafkaConfig(),
	); err != nil {
		return err
	}

	fmt.Fprintf(os.Stdout, "Processing messages\n")
	for msg := range consumer.Messages() {
		fmt.Println(string(msg.Data))
	}
	return nil
}

func doOnlineProcess(args []string, appConfg *config.Config) error {
	var (
		consumer types.Messager
		dec      *decoder.Decoder
		err      error
	)
	var (
		logHandle   = logging.NewLogHandler()
		kafkaConfig = appConfg.KafkaConfig()
	)

	// *TODO* move to logging package
	go func() {
		fmt.Fprintf(os.Stdout, "Starting notification handler\n")
		for not := range logHandle.Notifications() {
			switch v := not.(type) {
			case string:
				fmt.Fprintf(os.Stdout, "INFO: %s\n", v)
			case cluster.Notification:
				fmt.Fprintf(os.Stdout, "INFO: %+v\n", v)
			default:
			}
		}
		fmt.Fprintf(os.Stdout, "Stopping notification handler\n")
	}()
	// *TODO* move to logging package
	go func() {
		fmt.Fprintf(os.Stdout, "Starting error handler\n")
		for err := range logHandle.Errors() {
			// *TODO* error type switch
			fmt.Fprintf(os.Stderr, "ERROR: %s\n", err.Error())
		}
		fmt.Fprintf(os.Stdout, "Stopping error handler\n")
	}()

	kafkaConfig.LogHandler = logHandle
	fmt.Fprintf(os.Stdout, "Starting consumer\n")
	if consumer, err = kafka.NewKafkaIngest(
		kafkaConfig,
	); err != nil {
		return err
	}

	decoderConfig := appConfg.DecoderConfig(consumer, logHandle)
	if dec, err = decoder.NewMessageDecoder(
		*decoderConfig,
	); err != nil {
		return err
	}

	// *TODO* temp code during devel, remove
	fmt.Fprintf(os.Stdout, "Processing messages\n")
	for msg := range dec.Messages() {
		fmt.Println(string(msg.Data))
	}
	return nil
}

func doReplay(args []string, appConfg *config.Config) error {
	return nil
}

func printErr(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err.Error())
}
