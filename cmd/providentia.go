package cmd

import (
	"context"
	"encoding/json"
	"go-peek/internal/app"
	"go-peek/pkg/anonymizer"
	"go-peek/pkg/models/consumer"
	"go-peek/pkg/outputs/kafka"
	"go-peek/pkg/providentia"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// providentiaCmd represents the providentia command
var providentiaCmd = &cobra.Command{
	Use:   "providentia",
	Short: "Pull asset data from providentia API",
	Run: func(cmd *cobra.Command, args []string) {
		start := time.Now()
		logger.WithFields(logrus.Fields{}).Info("providentia called")

		defer logger.WithFields(logrus.Fields{"duration": time.Since(start)}).Info("All done!")
		defer app.Catch(logger)

		ticker := time.NewTicker(viper.GetDuration("providentia.interval"))
		defer ticker.Stop()

		var wg sync.WaitGroup
		defer wg.Wait()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tx := make(chan consumer.Message, 10)
		if viper.GetBool("providentia.output.kafka.enabled") {
			producer, err := kafka.NewProducer(&kafka.Config{
				Brokers: viper.GetStringSlice("providentia.output.kafka.brokers"),
				Logger:  logger,
			})
			app.Throw("Sarama producer init", err)
			topic := viper.GetString("providentia.output.kafka.topic")
			producer.Feed(tx, "Providentia producer", ctx, func(m consumer.Message) string {
				return topic
			}, &wg)
		}

		m, err := anonymizer.NewMapper()
		app.Throw("Anonymizer creation", err)

		fn := func() {
			if items, err := providentia.Pull(providentia.Params{
				URL:        viper.GetString("providentia.url"),
				Token:      viper.GetString("providentia.token"),
				Logger:     logger,
				Anonymizer: m,
			}); err != nil {
				logger.WithFields(logrus.Fields{}).Error(err)
			} else {
				ts := time.Now()
				logger.WithFields(logrus.Fields{
					"results": len(items),
					"url":     viper.GetString("providentia.url"),
				}).Debug("API call done")
				for _, item := range items {
					encoded, err := json.Marshal(item)
					app.Throw("Output JSON encode", err)
					if viper.GetBool("providentia.output.kafka.enabled") {
						tx <- consumer.Message{
							Data: encoded,
							Time: ts,
							Key:  "providentia",
						}
					}
				}
			}
		}

		fn()
		for {
			select {
			case <-ticker.C:
				fn()
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(providentiaCmd)

	providentiaCmd.PersistentFlags().String("url", "", "API endpoint")
	viper.BindPFlag("providentia.url", providentiaCmd.PersistentFlags().Lookup("url"))

	providentiaCmd.PersistentFlags().String("token", "", "API token")
	viper.BindPFlag("providentia.token", providentiaCmd.PersistentFlags().Lookup("token"))

	providentiaCmd.PersistentFlags().Duration("interval", 5*time.Minute, "Sleep between API calls")
	viper.BindPFlag("providentia.interval", providentiaCmd.PersistentFlags().Lookup("interval"))

	app.RegisterOutputKafka("providentia", providentiaCmd.PersistentFlags())
}