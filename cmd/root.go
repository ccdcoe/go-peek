package cmd

import (
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"go-peek/pkg/ingest"
	"go-peek/pkg/ingest/logfile"
	"go-peek/pkg/models/events"
	"go-peek/pkg/utils"

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
}

var pFlags = rootCmd.PersistentFlags()

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initLogging)
	cobra.OnInitialize(initConfig)

	pFlags.StringVar(&cfgFile, "config", "", "config file (default is $HOME/.peek.yaml)")
	pFlags.BoolVar(&debug, "debug", false, "Run in debug mode. Increases logging verbosity.")
	pFlags.BoolVar(&trace, "trace", false, "Run in trace mode. Log like a maniac.")

	pFlags.Int("work-threads", 2, "Number of threads for decoding messages")
	viper.BindPFlag("work.threads", pFlags.Lookup("work-threads"))

	pFlags.String("work-dir", path.Join(
		os.Getenv("HOME"),
		".local/peek",
	), "Working directory for storing dumps, temp files, etc.")
	viper.BindPFlag("work.dir", pFlags.Lookup("work-dir"))

	initInputConfig()
	initProcessorConfig()
	initStreamConfig()
	initOutputConfig("output")
	initOutputConfig("emit")
}

func initStreamConfig() {
	for _, stream := range events.Atomics {
		pFlags.StringSlice(
			fmt.Sprintf("stream-%s-dir", stream),
			[]string{},
			fmt.Sprintf("Source folder for event type %s. %s", stream, stream.Explain()),
		)
		viper.BindPFlag(
			fmt.Sprintf("stream.%s.dir", stream),
			pFlags.Lookup(
				fmt.Sprintf("stream-%s-dir", stream),
			),
		)

		pFlags.StringSlice(
			fmt.Sprintf("stream-%s-uxsock", stream),
			[]string{},
			fmt.Sprintf("Source unix socket for event type %s. %s", stream, stream.Explain()),
		)
		viper.BindPFlag(
			fmt.Sprintf("stream.%s.uxsock", stream),
			pFlags.Lookup(
				fmt.Sprintf("stream-%s-uxsock", stream),
			),
		)

		pFlags.StringSlice(
			fmt.Sprintf("stream-%s-kafka-topic", stream),
			[]string{},
			fmt.Sprintf("Source kafka topic for event type %s. %s", stream, stream.Explain()),
		)
		viper.BindPFlag(
			fmt.Sprintf("stream.%s.kafka.topic", stream),
			pFlags.Lookup(
				fmt.Sprintf("stream-%s-kafka-topic", stream),
			),
		)

		pFlags.String(
			fmt.Sprintf("stream-%s-parser", stream),
			"rfc5424",
			fmt.Sprintf("Parser for event type %s. Supported options are rfc5424 for IETF syslog formatted messages, json-raw for structured events, and json-game for meta-enritched events.", stream),
		)
		viper.BindPFlag(
			fmt.Sprintf("stream.%s.parser", stream),
			pFlags.Lookup(
				fmt.Sprintf("stream-%s-parser", stream),
			),
		)

	}
}

func initInputConfig() {
	// Kafka consumer
	pFlags.Bool("input-kafka-enabled", false,
		fmt.Sprintf(`Enable kafka consumer. %s`, ingest.Kafka.Explain()))
	viper.BindPFlag("input.kafka.enabled", pFlags.Lookup("input-kafka-enabled"))

	pFlags.StringSlice("input-kafka-host", []string{"localhost:9092"},
		`Kafka bootstrap broker for consumer. Can be specified multiple times to use a cluster.`)
	viper.BindPFlag("input.kafka.host", pFlags.Lookup("input-kafka-host"))

	pFlags.String("input-kafka-group", "peek",
		"Kafka consumer group for maintaining offsets.")
	viper.BindPFlag("input.kafka.group", pFlags.Lookup("input-kafka-group"))

	pFlags.String("input-kafka-mode", "follow",
		`Where to begin consuming. Valid options:
		follow - continue from last committed offset for consumer group
		beginning - start from first available message in topic
		latest - start from most recent message in topic`)
	viper.BindPFlag("input.kafka.mode", pFlags.Lookup("input-kafka-mode"))

	// Directory consumer
	pFlags.Bool("input-dir-enabled", false,
		`Enable reading compressed or plaintext log files from directory. For post-mortem processing`)
	viper.BindPFlag("input.dir.enabled", pFlags.Lookup("input-dir-enabled"))

	// Unix socket consumer
	pFlags.Bool("input-uxsock-enabled", false,
		`Enable reading from unix sockets. Sockets will be created and cleaned up by peek process.`)
	viper.BindPFlag("input.uxsock.enabled", pFlags.Lookup("input-uxsock-enabled"))

	pFlags.Bool("input-uxsock-overwrite", false,
		`Delete existing file if socket path already exists.`)
	viper.BindPFlag("input.uxsock.overwrite", pFlags.Lookup("input-uxsock-overwrite"))
}

func initProcessorConfig() {
	pFlags.Bool("processor-enabled", true,
		`Enable or disable all processor plugins globally.`)
	viper.BindPFlag("processor.enabled", pFlags.Lookup("processor-enabled"))

	pFlags.Bool("processor-sigma-enabled", false,
		`Enable sigma rule engine.`)
	viper.BindPFlag("processor.sigma.enabled", pFlags.Lookup("processor-sigma-enabled"))

	pFlags.StringSlice("processor-sigma-dir", []string{},
		`Directories that contains sigma rules. Multiple directories can be defined. `+
			`Each directory will be scored recursively for files with "yml" suffix.`)
	viper.BindPFlag("processor.sigma.dir", pFlags.Lookup("processor-sigma-dir"))

	pFlags.Bool("processor-mitre-enabled", false,
		`JSON file containing MITRE att&ck ID to technique and phase mapping.`)
	viper.BindPFlag(
		"processor.mitre.enabled",
		pFlags.Lookup("processor-mitre-enabled"),
	)
	pFlags.String("processor-mitre-json-enterprise", "",
		`JSON file containing MITRE att&ck ID to technique and phase mapping.`)
	viper.BindPFlag(
		"processor.mitre.json.enterprise",
		pFlags.Lookup("processor-mitre-json-enterprise"),
	)
	pFlags.Bool("processor-mitre-meerkat-enabled", false, `Map suricata SID to mitre values from database.`)
	viper.BindPFlag("processor.mitre.meerkat.enabled", pFlags.Lookup("processor-mitre-meerkat-enabled"))

	pFlags.String("processor-mitre-meerkat-redis-host", "localhost", `Redis host for mitre meerkat mappings.`)
	viper.BindPFlag("processor.mitre.meerkat.redis.host", pFlags.Lookup("processor-mitre-meerkat-redis-host"))

	pFlags.Int("processor-mitre-meerkat-redis-port", 6379, `Redis port for mitre meerkat mappings.`)
	viper.BindPFlag("processor.mitre.meerkat.redis.port", pFlags.Lookup("processor-mitre-meerkat-redis-port"))

	pFlags.Int("processor-mitre-meerkat-redis-db", 0, `Redis db for mitre meerkat mappings.`)
	viper.BindPFlag("processor.mitre.meerkat.redis.db", pFlags.Lookup("processor-mitre-meerkat-redis-db"))

	pFlags.Bool("processor-assets-enabled", false,
		`Enable asset tracking.`)
	viper.BindPFlag("processor.assets.enabled", pFlags.Lookup("processor-assets-enabled"))

	pFlags.String("processor-assets-kafka-topic", "assets",
		`Kafka topic for asset data.`)
	viper.BindPFlag("processor.assets.kafka.topic", pFlags.Lookup("processor-assets-kafka-topic"))

	pFlags.StringSlice("processor-assets-kafka-host", []string{"localhost:9092"},
		`Kafka bootstrap broker for consumer. Can be specified multiple times to use a cluster.`)
	viper.BindPFlag("processor.assets.kafka.host", pFlags.Lookup("processor-assets-kafka-host"))

	pFlags.String("processor-assets-kafka-group", "peek",
		"Kafka consumer group for maintaining offsets.")
	viper.BindPFlag("processor.assets.kafka.group", pFlags.Lookup("processor-assets-kafka-group"))

	pFlags.String("processor-assets-kafka-mode", "follow",
		`Where to begin consuming. Valid options:
		follow - continue from last committed offset for consumer group
		beginning - start from first available message in topic
		latest - start from most recent message in topic`)
	viper.BindPFlag("processor.assets.kafka.mode", pFlags.Lookup("processor-assets-kafka-mode"))

	pFlags.Bool("processor-assets-redis-enabled", false,
		`Enable asset tracking.`)
	viper.BindPFlag("processor.assets.redis.enabled", pFlags.Lookup("processor-assets-redis-enabled"))

	pFlags.String("processor-assets-redis-host", "localhost", `Redis host for asset mappings.`)
	viper.BindPFlag("processor.mitre.meerkat.redis.host", pFlags.Lookup("processor-assets-redis-host"))

	pFlags.Int("processor-assets-redis-port", 6379, `Redis port for asset mappings.`)
	viper.BindPFlag("processor.mitre.meerkat.redis.port", pFlags.Lookup("processor-assets-redis-port"))

	pFlags.Int("processor-assets-redis-db", 0, `Redis db for asset mappings.`)
	viper.BindPFlag("processor.mitre.meerkat.redis.db", pFlags.Lookup("processor-assets-redis-db"))
}

func initOutputConfig(prefix string) {
	// Elastic
	pFlags.Bool(prefix+"-elastic-enabled", false,
		`Enable elasticsearch output.`)
	viper.BindPFlag(prefix+".elastic.enabled", pFlags.Lookup(prefix+"-elastic-enabled"))

	pFlags.StringSlice(prefix+"-elastic-host", []string{"http://localhost:9200"},
		`Elasticsearch http proxy host. Can be specified multiple times to use a cluster.`)
	viper.BindPFlag(prefix+".elastic.host", pFlags.Lookup(prefix+"-elastic-host"))

	pFlags.String(prefix+"-elastic-prefix", "events",
		`Prefix for all index patterns. For example Suricata events would follow a pattern <prefix>-suricata-YYYY.MM.DD`)
	viper.BindPFlag(prefix+".elastic.prefix", pFlags.Lookup(prefix+"-elastic-prefix"))

	pFlags.Bool(prefix+"-elastic-merge", false,
		`Send all messages to a single index pattern, as opposed to creating an index per event type.`)
	viper.BindPFlag(prefix+".elastic.merge", pFlags.Lookup(prefix+"-elastic-merge"))

	pFlags.Bool(prefix+"-elastic-hourly", false,
		`Hourly index pattern as opposed to daily. In other word, new index would be created every hour. Avoid in production, will explode your shard count.`)
	viper.BindPFlag(prefix+".elastic.hourly", pFlags.Lookup(prefix+"-elastic-hourly"))

	pFlags.Int(prefix+"-elastic-threads", viper.GetInt("work.threads"),
		`Number of workers. Can be useful for increasing throughput when elastic cluster has a lot of resources.`)
	viper.BindPFlag(prefix+".elastic.threads", pFlags.Lookup(prefix+"-elastic-threads"))

	// Kafka
	pFlags.Bool(prefix+"-kafka-enabled", false,
		`Enable kafka producer.`)
	viper.BindPFlag(prefix+".kafka.enabled", pFlags.Lookup(prefix+"-kafka-enabled"))

	pFlags.StringSlice(prefix+"-kafka-host", []string{"localhost:9092"},
		`Kafka bootstrap broker for producer. Can be specified multiple times to use a cluster.`)
	viper.BindPFlag(prefix+".kafka.host", pFlags.Lookup(prefix+"-kafka-host"))

	pFlags.String(prefix+"-kafka-prefix", "events",
		`Prefix for topic names. For example Suricata events would be sent to <prefix>-suricata`)
	viper.BindPFlag(prefix+".kafka.prefix", pFlags.Lookup(prefix+"-kafka-prefix"))

	pFlags.String(prefix+"-kafka-topic", "",
		`Optional topic name for producing messages. Applies on all streams and overrides --output-kafka-prefix and --output-kafka-merge parameters. Meant for simple scenarios when dynamic stream splitting is not needed.`)
	viper.BindPFlag(prefix+".kafka.topic", pFlags.Lookup(prefix+"-kafka-topic"))

	pFlags.Bool(prefix+"-kafka-merge", false,
		`Send all messages to a single topic, as opposed to topic per event type.`)
	viper.BindPFlag(prefix+".kafka.merge", pFlags.Lookup(prefix+"-kafka-merge"))

	// fifo
	pFlags.StringSlice(prefix+"-fifo-path", []string{},
		`Named pipe, or FIFO, for outputting event messages. Multiple outputs can be specified.`)
	viper.BindPFlag(prefix+".fifo.path", pFlags.Lookup(prefix+"-fifo-path"))

	// stdout
	pFlags.Bool(prefix+"-stdout", false,
		`Print output messages to stdout. Good for simple cli piping and debug.`)
	viper.BindPFlag(prefix+".stdout", pFlags.Lookup(prefix+"-stdout"))

	// regular file output
	pFlags.Bool(prefix+"-file-enabled", false,
		`Write all messages to single file. Good for creating and archive.`)
	viper.BindPFlag(prefix+".file.enabled", pFlags.Lookup(prefix+"-file-enabled"))

	pFlags.String(prefix+"-file-path", "",
		`Path for output file.`)
	viper.BindPFlag(prefix+".file.path", pFlags.Lookup(prefix+"-file-path"))

	pFlags.String(prefix+"-file-dir", "",
		`Direcotry for sorted output files.`)
	viper.BindPFlag(prefix+".file.dir", pFlags.Lookup(prefix+"-file-dir"))

	pFlags.Bool(prefix+"-file-gzip", false,
		`Write directly to gzip file. Reduces disk usage by approximately 90 per cent. Cannot be used together with --output-file-rotate-enabled.`)
	viper.BindPFlag(prefix+".file.gzip", pFlags.Lookup(prefix+"-file-gzip"))

	pFlags.Bool(prefix+"-file-timestamp", false,
		`Append timestamp to output file name. Useful for keeping track in case consumer breaks.`)
	viper.BindPFlag(prefix+".file.timestamp", pFlags.Lookup(prefix+"-file-timestamp"))

	pFlags.Bool(prefix+"-file-rotate-enabled", false,
		`Enable periodic log file rotation.`)
	viper.BindPFlag(prefix+".file.rotate.enabled", pFlags.Lookup(prefix+"-file-rotate-enabled"))

	pFlags.Bool(prefix+"-file-rotate-gzip", false,
		`Gzip compress log files post-rotation.`)
	viper.BindPFlag(prefix+".file.rotate.gzip", pFlags.Lookup(prefix+"-file-rotate-gzip"))

	pFlags.Duration(prefix+"-file-rotate-interval", 1*time.Hour,
		`Interval for rotating output files if enabled.`)
	viper.BindPFlag(prefix+".file.rotate.interval", pFlags.Lookup(prefix+"-file-rotate-interval"))
}

func initLogging() {
	//log.SetFormatter(&log.JSONFormatter{})
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
