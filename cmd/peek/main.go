package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/BurntSushi/toml"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/ccdcoe/go-peek/internal/config"
	"github.com/ccdcoe/go-peek/internal/decoder"
	"github.com/ccdcoe/go-peek/internal/ingest/kafka"
	"github.com/ccdcoe/go-peek/internal/logging"
	"github.com/ccdcoe/go-peek/internal/types"
	"github.com/ccdcoe/go-peek/pkg/outputs"
)

const argTsFormat = "2006-01-02 15:04:05"

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
		fmt.Fprintf(os.Stdout, "%s\n", msg.String())
	}
	return nil
}

func doOnlineProcess(args []string, appConfg *config.Config) error {
	var (
		consumer types.Messager
		dec      *decoder.Decoder
		err      error
	)

	logHandle := logging.NewLogHandler()
	kafkaConfig := appConfg.KafkaConfig()
	logs(logHandle)

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

	fmt.Fprintf(os.Stdout, "Processing messages\n")
	outConfig := appConfg.OutputConfig(logHandle)
	out := outputs.Output(dec.Messages())
	out.Produce(*outConfig, context.Background())
	outConfig.Wait.Wait()

	return nil
}

var (
	replayFlags = flag.NewFlagSet("replay", flag.ExitOnError)

	timeFrom = replayFlags.String("time-from", "2018-11-30 00:00:00",
		`Process messages with timestamps > value. Format is YYYY-MM-DD HH:mm:ss`)
	timeTo = replayFlags.String("time-to", "2018-12-07 00:00:00",
		`Process messages with timestamps < value. Format is YYYY-MM-DD HH:mm:ss`)
	speedup = replayFlags.Int64("ff", 1,
		`Fast forward x times`)
	statTimeout = replayFlags.Duration("stat-timeout", 60*time.Minute,
		`Timeout for statting logfile stats. NOT FULLY IMPLEMENTED AND BUGGY!!! USE A HIGH VALUE!!!`)
	stdout = replayFlags.Bool("stdout", false,
		`Print messages to standard output, as opposed to producing to kafka.`)
	stdoutRaw = replayFlags.Bool("stdout-raw", false,
		`Used in conjunction to -stdout. Print additional message info, as opposed to textual payload.`)
)

func doReplay(args []string, appConfg *config.Config) error {
	if err := replayFlags.Parse(args); err != nil {
		return err
	}
	args = replayFlags.Args()
	logHandle := logging.NewLogHandler()
	logs(logHandle)

	interval := func(start, stop string) (time.Time, time.Time, error) {
		from, err := time.Parse(argTsFormat, *timeFrom)
		if err != nil {
			return time.Now(), time.Now(), err
		}
		to, err := time.Parse(argTsFormat, *timeTo)
		if err != nil {
			return time.Now(), time.Now(), err
		}
		if from.UnixNano() > to.UnixNano() {
			return time.Now(), time.Now(), fmt.Errorf("from > to")
		}
		return from, to, nil
	}
	from, to, err := interval(*timeFrom, *timeTo)
	if err != nil {
		return err
	}

	buildTimeListAndReplay := func() (types.Messager, decoder.MultiFileInfoListing, error) {
		logStatConfig := appConfg.GetReplayStatConfig(logHandle, from, to, *statTimeout)
		sources, err := decoder.MultiListLogFilesAndStatEventStart(
			logStatConfig,
		)
		if err != nil {
			return nil, nil, err
		}
		replayConfig := decoder.LogReplayWorkerConfig{
			From:    from,
			To:      to,
			Logger:  logHandle,
			Workers: int(appConfg.General.Workers),
			Timeout: *statTimeout,
			Speedup: int64(*speedup),
		}
		timelistmap, err := sources.CollectTimeStamps(replayConfig)
		if err != nil {
			return nil, nil, err
		}
		messages, err := timelistmap.Replay(replayConfig)
		if err != nil {
			return nil, nil, err
		}
		return messages, sources, nil
	}
	messages, logMap, err := buildTimeListAndReplay()
	if err != nil {
		return err
	}

	decoderConfig := appConfg.DecoderConfig(messages, logHandle)
	decoderConfig.EventMap = logMap.EventTypeMap()
	dec, err := decoder.NewMessageDecoder(*decoderConfig)
	if err != nil {
		return err
	}

	if *stdout {
		for msg := range dec.Messages() {
			if *stdoutRaw {
				fmt.Fprintf(os.Stdout, "%s %d %s\n", msg.Source, msg.Offset, msg.String())
			} else {
				fmt.Fprintf(os.Stdout, "%s\n", msg.String())
			}
		}
	} else {
		kafkaConfig := appConfg.KafkaConfig()
		kafkaConfig.LogHandler = logHandle

		fmt.Fprintf(os.Stdout, "Processing messages\n")
		outConfig := appConfg.OutputConfig(logHandle)
		outConfig.TopicMap = outConfig.TopicMap.MapSources(logMap.EventMap())

		out := outputs.Output(dec.Messages())
		out.Produce(*outConfig, context.Background())

		outConfig.Wait.Wait()
	}

	return nil
}

func logs(logHandle logging.LogListener) error {
	errs := func() {
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
	}
	msgs := func() {
		fmt.Fprintf(os.Stdout, "Starting error handler\n")
		for err := range logHandle.Errors() {
			// *TODO* error type switch
			fmt.Fprintf(os.Stderr, "ERROR: %s\n", err.Error())
		}
		fmt.Fprintf(os.Stdout, "Stopping error handler\n")
	}
	go errs()
	go msgs()

	return nil
}
