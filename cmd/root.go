package cmd

import (
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/ccdcoe/go-peek/pkg/ingest"
	"github.com/ccdcoe/go-peek/pkg/ingest/logfile"
	"github.com/ccdcoe/go-peek/pkg/models/events"
	"github.com/ccdcoe/go-peek/pkg/utils"
	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

var (
	cfgFile      string
	debug, trace bool
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "peek",
	Short: "Simple streaming pre-processor and enrichment tool for structured logs.",
	Long: `Examples:

		TODO`,
	//Run: doRun,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
func doRun(cmd *cobra.Command, args []string) {
	log.Info("Starting peek")
}

func init() {
	cobra.OnInitialize(initLogging)
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.peek.yaml)")
	rootCmd.PersistentFlags().BoolVar(&debug, "debug", false, "Run in debug mode. Increases logging verbosity.")
	rootCmd.PersistentFlags().BoolVar(&trace, "trace", false, "Run in trace mode. Log like a maniac.")

	rootCmd.PersistentFlags().Int("work-threads", 2, "Number of threads for decoding messages")
	viper.BindPFlag("work.threads", rootCmd.PersistentFlags().Lookup("work-threads"))

	rootCmd.PersistentFlags().String("work-dir", path.Join(
		os.Getenv("HOME"),
		".local/peek",
	), "Working directory for storing dumps, temp files, etc.")
	viper.BindPFlag("work.dir", rootCmd.PersistentFlags().Lookup("work-dir"))

	initInputConfig()
	initProcessorConfig()
	initStreamConfig()
	initOutputConfig()
}

func initStreamConfig() {
	for _, stream := range events.Atomics {
		rootCmd.PersistentFlags().StringSlice(
			fmt.Sprintf("stream-%s-dir", stream),
			[]string{},
			fmt.Sprintf("Source folder for event type %s. %s", stream, stream.Explain()),
		)
		viper.BindPFlag(
			fmt.Sprintf("stream.%s.dir", stream),
			rootCmd.PersistentFlags().Lookup(
				fmt.Sprintf("stream-%s-dir", stream),
			),
		)

		rootCmd.PersistentFlags().StringSlice(
			fmt.Sprintf("stream-%s-uxsock", stream),
			[]string{},
			fmt.Sprintf("Source unix socket for event type %s. %s", stream, stream.Explain()),
		)
		viper.BindPFlag(
			fmt.Sprintf("stream.%s.uxsock", stream),
			rootCmd.PersistentFlags().Lookup(
				fmt.Sprintf("stream-%s-uxsock", stream),
			),
		)

		rootCmd.PersistentFlags().StringSlice(
			fmt.Sprintf("stream-%s-kafka-topic", stream),
			[]string{},
			fmt.Sprintf("Source kafka topic for event type %s. %s", stream, stream.Explain()),
		)
		viper.BindPFlag(
			fmt.Sprintf("stream.%s.kafka.topic", stream),
			rootCmd.PersistentFlags().Lookup(
				fmt.Sprintf("stream-%s-kafka-topic", stream),
			),
		)

		rootCmd.PersistentFlags().String(
			fmt.Sprintf("stream-%s-parser", stream),
			"rfc5424",
			fmt.Sprintf("Parser for event type %s. Supported options are rfc5424 for IETF syslog formatted messages, json-raw for structured events, and json-game for meta-enritched events.", stream),
		)
		viper.BindPFlag(
			fmt.Sprintf("stream.%s.parser", stream),
			rootCmd.PersistentFlags().Lookup(
				fmt.Sprintf("stream-%s-parser", stream),
			),
		)

	}
}

func initInputConfig() {
	// Kafka consumer
	rootCmd.PersistentFlags().Bool("input-kafka-enabled", false,
		fmt.Sprintf(`Enable kafka consumer. %s`, ingest.Kafka.Explain()))
	viper.BindPFlag("input.kafka.enabled", rootCmd.PersistentFlags().Lookup("input-kafka-enabled"))

	rootCmd.PersistentFlags().StringSlice("input-kafka-host", []string{"localhost:9092"},
		`Kafka bootstrap broker for consumer. Can be specified multiple times to use a cluster.`)
	viper.BindPFlag("input.kafka.host", rootCmd.PersistentFlags().Lookup("input-kafka-host"))

	rootCmd.PersistentFlags().String("input-kafka-group", "peek",
		"Kafka consumer group for maintaining offsets.")
	viper.BindPFlag("input.kafka.group", rootCmd.PersistentFlags().Lookup("input-kafka-group"))

	rootCmd.PersistentFlags().Bool("input-kafka-commit", true,
		`Commit offsets under to the broker. To continue from last commit in case consumer is stopped.`)
	viper.BindPFlag("input.kafka.commit", rootCmd.PersistentFlags().Lookup("input-kafka-commit"))

	rootCmd.PersistentFlags().String("input-kafka-mode", "follow",
		`Where to begin consuming. Valid options:
		follow - continue from last committed offset for consumer group
		beginning - start from first available message in topic
		latest - start from most recent message in topic`)
	viper.BindPFlag("input.kafka.mode", rootCmd.PersistentFlags().Lookup("input-kafka-mode"))

	// Directory consumer
	rootCmd.PersistentFlags().Bool("input-dir-enabled", false,
		`Enable reading compressed or plaintext log files from directory. For post-mortem processing`)
	viper.BindPFlag("input.dir.enabled", rootCmd.PersistentFlags().Lookup("input-dir-enabled"))

	// Unix socket consumer
	rootCmd.PersistentFlags().Bool("input-uxsock-enabled", false,
		`Enable reading from unix sockets. Sockets will be created and cleaned up by peek process.`)
	viper.BindPFlag("input.uxsock.enabled", rootCmd.PersistentFlags().Lookup("input-uxsock-enabled"))

	rootCmd.PersistentFlags().Bool("input-uxsock-overwrite", false,
		`Delete existing file if socket path already exists.`)
	viper.BindPFlag("input.uxsock.overwrite", rootCmd.PersistentFlags().Lookup("input-uxsock-overwrite"))
}

func initProcessorConfig() {
	rootCmd.PersistentFlags().Bool("processor-enabled", true,
		`Enable or disable all processor plugins globally.`)
	viper.BindPFlag("processor.enabled", rootCmd.PersistentFlags().Lookup("processor-enabled"))

	rootCmd.PersistentFlags().Bool("processor-anonymize", false,
		`Anonymize messages. Simple method by replacing host names with aliases`)
	viper.BindPFlag("processor.anonymize", rootCmd.PersistentFlags().Lookup("processor-anonymize"))

	rootCmd.PersistentFlags().Bool("processor-inputs-wise-enabled", false,
		`Enable or disable WISE asset lookups.`)
	viper.BindPFlag("processor.inputs.wise.enabled", rootCmd.PersistentFlags().Lookup("processor-inputs-wise-enabled"))

	rootCmd.PersistentFlags().String("processor-inputs-wise-host", "http://localhost:8081",
		`Remote Moloch WISE host that holds asset and IOC information.`)
	viper.BindPFlag("processor.inputs.wise.host", rootCmd.PersistentFlags().Lookup("processor-inputs-wise-host"))

	rootCmd.PersistentFlags().String("processor-inputs-redis-host", "localhost",
		`Redis host for collecting asset and threat intel.`)
	viper.BindPFlag("processor.inputs.redis.host", rootCmd.PersistentFlags().Lookup("processor-inputs-redis-host"))

	rootCmd.PersistentFlags().Int("processor-inputs-redis-port", 6379,
		`Redis port for collecting asset and threat intel.`)
	viper.BindPFlag("processor.inputs.redis.port", rootCmd.PersistentFlags().Lookup("processor-inputs-redis-port"))

	rootCmd.PersistentFlags().Int("processor-inputs-redis-db", 0,
		`Redis database for collecting asset and threat intel.`)
	viper.BindPFlag("processor.inputs.redis.db", rootCmd.PersistentFlags().Lookup("processor-inputs-redis-db"))

	rootCmd.PersistentFlags().Bool("processor-sigma-enabled", false,
		`Enable sigma rule engine.`)
	viper.BindPFlag("processor.sigma.enabled", rootCmd.PersistentFlags().Lookup("processor-sigma-enabled"))

	rootCmd.PersistentFlags().StringSlice("processor-sigma-dir", []string{},
		`Directories that contains sigma rules. Multiple directories can be defined. `+
			`Each directory will be scored recursively for files with "yml" suffix.`)
	viper.BindPFlag("processor.sigma.dir", rootCmd.PersistentFlags().Lookup("processor-sigma-dir"))

	rootCmd.PersistentFlags().Bool("processor-sigma-quickmatch", false,
		`Stop matching on event upon first positive result. `+
			`Otherwise whole rule group will be checked for each event belonging to that group.`)
	viper.BindPFlag("processor.sigma.quickmatch", rootCmd.PersistentFlags().Lookup("processor-sigma-quickmatch"))
}

func initOutputConfig() {
	// Elastic
	rootCmd.PersistentFlags().Bool("output-elastic-enabled", false,
		`Enable elasticsearch output.`)
	viper.BindPFlag("output.elastic.enabled", rootCmd.PersistentFlags().Lookup("output-elastic-enabled"))

	rootCmd.PersistentFlags().StringSlice("output-elastic-host", []string{"http://localhost:9200"},
		`Elasticsearch http proxy host. Can be specified multiple times to use a cluster.`)
	viper.BindPFlag("output.elastic.host", rootCmd.PersistentFlags().Lookup("output-elastic-host"))

	rootCmd.PersistentFlags().String("output-elastic-prefix", "events",
		`Prefix for all index patterns. For example Suricata events would follow a pattern <prefix>-suricata-YYYY.MM.DD`)
	viper.BindPFlag("output.elastic.prefix", rootCmd.PersistentFlags().Lookup("output-elastic-prefix"))

	rootCmd.PersistentFlags().Bool("output-elastic-merge", false,
		`Send all messages to a single index pattern, as opposed to creating an index per event type.`)
	viper.BindPFlag("output.elastic.merge", rootCmd.PersistentFlags().Lookup("output-elastic-merge"))

	rootCmd.PersistentFlags().Bool("output-elastic-hourly", false,
		`Hourly index pattern as opposed to daily. In other word, new index would be created every hour. Avoid in production, will explode your shard count.`)
	viper.BindPFlag("output.elastic.hourly", rootCmd.PersistentFlags().Lookup("output-elastic-hourly"))

	rootCmd.PersistentFlags().Int("output-elastic-threads", viper.GetInt("work.threads"),
		`Number of workers. Can be useful for increasing throughput when elastic cluster has a lot of resources.`)
	viper.BindPFlag("output.elastic.threads", rootCmd.PersistentFlags().Lookup("output-elastic-threads"))

	// Kafka
	rootCmd.PersistentFlags().Bool("output-kafka-enabled", false,
		`Enable kafka producer.`)
	viper.BindPFlag("output.kafka.enabled", rootCmd.PersistentFlags().Lookup("output-kafka-enabled"))

	rootCmd.PersistentFlags().StringSlice("output-kafka-host", []string{"localhost:9092"},
		`Kafka bootstrap broker for producer. Can be specified multiple times to use a cluster.`)
	viper.BindPFlag("output.kafka.host", rootCmd.PersistentFlags().Lookup("output-kafka-host"))

	rootCmd.PersistentFlags().String("output-kafka-prefix", "events",
		`Prefix for topic names. For example Suricata events would be sent to <prefix>-suricata`)
	viper.BindPFlag("output.kafka.prefix", rootCmd.PersistentFlags().Lookup("output-kafka-prefix"))

	rootCmd.PersistentFlags().String("output-kafka-topic", "",
		`Optional topic name for producing messages. Applies on all streams and overrides --output-kafka-prefix and --output-kafka-merge parameters. Meant for simple scenarios when dynamic stream splitting is not needed.`)
	viper.BindPFlag("output.kafka.topic", rootCmd.PersistentFlags().Lookup("output-kafka-topic"))

	rootCmd.PersistentFlags().Bool("output-kafka-merge", false,
		`Send all messages to a single topic, as opposed to topic per event type.`)
	viper.BindPFlag("output.kafka.merge", rootCmd.PersistentFlags().Lookup("output-kafka-merge"))

	// fifo
	rootCmd.PersistentFlags().StringSlice("output-fifo-path", []string{},
		`Named pipe, or FIFO, for outputting event messages. Multiple outputs can be specified.`)
	viper.BindPFlag("output.fifo.path", rootCmd.PersistentFlags().Lookup("output-fifo-path"))

	// stdout
	rootCmd.PersistentFlags().Bool("output-stdout", false,
		`Print output messages to stdout. Good for simple cli piping and debug.`)
	viper.BindPFlag("output.stdout", rootCmd.PersistentFlags().Lookup("output-stdout"))

	// regular file output
	rootCmd.PersistentFlags().Bool("output-file-enabled", false,
		`Write all messages to single file. Good for creating and archive.`)
	viper.BindPFlag("output.file.enabled", rootCmd.PersistentFlags().Lookup("output-file-enabled"))

	rootCmd.PersistentFlags().String("output-file-path", "",
		`Path for output file.`)
	viper.BindPFlag("output.file.path", rootCmd.PersistentFlags().Lookup("output-file-path"))

	rootCmd.PersistentFlags().String("output-file-dir", "",
		`Direcotry for sorted output files.`)
	viper.BindPFlag("output.file.dir", rootCmd.PersistentFlags().Lookup("output-file-dir"))

	rootCmd.PersistentFlags().Bool("output-file-gzip", false,
		`Write directly to gzip file. Reduces disk usage by approximately 90 per cent. Cannot be used together with --output-file-rotate-enabled.`)
	viper.BindPFlag("output.file.gzip", rootCmd.PersistentFlags().Lookup("output-file-gzip"))

	rootCmd.PersistentFlags().Bool("output-file-timestamp", false,
		`Append timestamp to output file name. Useful for keeping track in case consumer breaks.`)
	viper.BindPFlag("output.file.timestamp", rootCmd.PersistentFlags().Lookup("output-file-timestamp"))

	rootCmd.PersistentFlags().Bool("output-file-rotate-enabled", false,
		`Enable periodic log file rotation.`)
	viper.BindPFlag("output.file.rotate.enabled", rootCmd.PersistentFlags().Lookup("output-file-rotate-enabled"))

	rootCmd.PersistentFlags().Bool("output-file-rotate-gzip", false,
		`Gzip compress log files post-rotation.`)
	viper.BindPFlag("output.file.rotate.gzip", rootCmd.PersistentFlags().Lookup("output-file-rotate-gzip"))

	rootCmd.PersistentFlags().Duration("output-file-rotate-interval", 1*time.Hour,
		`Interval for rotating output files if enabled with --output-file-rotate-enabled.`)
	viper.BindPFlag("output.file.rotate.interval", rootCmd.PersistentFlags().Lookup("output-file-rotate-interval"))
}

func initLogging() {
	log.SetFormatter(&log.JSONFormatter{})
	if debug && !trace {
		log.Info("Setting log level to debug")
		log.SetLevel(log.DebugLevel)
	}
	if trace {
		log.Info("Setting log level to trace")
		log.SetLevel(log.TraceLevel)
	}
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".peek" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".peek")
	}

	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetEnvPrefix("peek")
	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Info("Using config file: ", viper.ConfigFileUsed())
	}
}

// TODO - use more
func errLogger(err error, exit bool) {
	switch v := err.(type) {
	case utils.ErrChan:
		if l := log.GetLevel(); l >= log.DebugLevel {
			for i := 0; i < len(v.Items); i++ {
				errLogger(<-v.Items, false)
			}
			log.Debug("Done iterating errors")
		} else {
			log.WithField("type", "err channel").Error(v)
		}
	case *utils.ErrInvalidInterval, utils.ErrInvalidInterval:
		log.WithField("type", "invalid interval").Error(v)
	case *logfile.ErrEmptyCollect, logfile.ErrEmptyCollect:
		log.WithField("type", "empty collect").Error(v)
	case *utils.ErrFuncMissing, utils.ErrFuncMissing:
		log.WithField("type", "missing func").Error(v)
	case *logfile.ErrUnknownGeneratorType, logfile.ErrUnknownGeneratorType:
		log.WithField("type", "unk generator type").Error(v)
	default:
		log.WithField("type", "not custom").Error(v)
	}
	if exit {
		os.Exit(1)
	}
}
