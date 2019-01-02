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

const (
	onliemode = "online"
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
		command  string
		args     []string
		commandF func(args []string, conf *config.Config) error
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

	args = mainFlags.Args()
	if len(args) == 0 {
		fmt.Fprintf(os.Stdout, "No command set, assiming online\n")
		command = onliemode
	} else {
		command = args[0]
		args = args[1:]
	}

	switch command {
	case onliemode:
		commandF = doOnlineConsume
	case "replay":
		commandF = doReplay
	default:
		// *TODO* print list of commands
		fmt.Fprintf(os.Stderr, "Unknown command, exiting\n")
		os.Exit(2)
	}

	fmt.Fprintf(os.Stdout, "Running main command\n")
	if err = commandF(args, appConfg); err != nil {
		// *TODO* error type switch here
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(2)
	}
}

func doOnlineConsume(args []string, appConfg *config.Config) error {
	var (
		consumer ingest.Ingester
		err      error
	)

	fmt.Fprintf(os.Stdout, "Starting consumer\n")
	if consumer, err = kafka.NewKafkaIngest(
		appConfg.KafkaConfig(),
	); err != nil {
		return err
	}

	// *TODO* temp code during devel, remove
	fmt.Fprintf(os.Stdout, "Processing messages\n")
	for msg := range consumer.Messages() {
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
