package cmd

import (
	"fmt"
	"os"
	"path"

	"github.com/ccdcoe/go-peek/internal/ingest/v2/logfile"
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
	), "Number of threads for decoding messages")
	viper.BindPFlag("work.dir", rootCmd.PersistentFlags().Lookup("work-dir"))

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
	}
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

	// Kafka
	rootCmd.PersistentFlags().Bool("output-kafka-enabled", false,
		`Enable elasticsearch output.`)
	viper.BindPFlag("output.kafka.enabled", rootCmd.PersistentFlags().Lookup("output-kafka-enabled"))

	rootCmd.PersistentFlags().StringSlice("output-kafka-host", []string{"localhost:9092"},
		`Kafka bootstrap broker. Can be specified multiple times to use a cluster.`)
	viper.BindPFlag("output.kafka.host", rootCmd.PersistentFlags().Lookup("output-kafka-host"))

	rootCmd.PersistentFlags().String("output-kafka-prefix", "events",
		`Prefix for topic names. For example Suricata events would be sent to <prefix>-suricata`)
	viper.BindPFlag("output.kafka.prefix", rootCmd.PersistentFlags().Lookup("output-kafka-prefix"))

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
		if debug || trace {
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
	case *logfile.ErrFuncMissing, logfile.ErrFuncMissing:
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
