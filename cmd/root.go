package cmd

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/spf13/cobra"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

var (
	cfgFile      string
	debug, trace bool
)

var logger = logrus.New()

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "peek",
	Short: "Simple streaming pre-processor and enrichment tool for structured logs.",
}

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

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.peek.yaml)")
	rootCmd.PersistentFlags().BoolVar(&debug, "debug", false, "Run in debug mode. Increases logging verbosity.")
	rootCmd.PersistentFlags().BoolVar(&trace, "trace", false, "Run in trace mode. Log like a maniac.")

	rootCmd.PersistentFlags().String("work-dir", path.Join(
		os.Getenv("HOME"),
		".local/peek",
	), "Working directory for storing dumps, temp files, etc.")
	viper.BindPFlag("work.dir", rootCmd.PersistentFlags().Lookup("work-dir"))
}

func initLogging() {
	//log.SetFormatter(&log.JSONFormatter{})
	if debug {
		logger.Info("Setting log level to debug")
		logger.SetLevel(logrus.DebugLevel)
	} else if trace {
		logger.Info("Setting log level to trace")
		logger.SetLevel(logrus.TraceLevel)
	} else {
		logger.Info("Setting log level to info")
		logger.SetLevel(logrus.InfoLevel)
	}
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
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
			logger.Fatal(err)
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
		logger.Info("Using config file: ", viper.ConfigFileUsed())
	}
}
