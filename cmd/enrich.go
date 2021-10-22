package cmd

import (
	"context"
	"go-peek/internal/app"
	"go-peek/pkg/enrich"
	"go-peek/pkg/ingest/kafka"
	"go-peek/pkg/models/consumer"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// enrichCmd represents the enrich command
var enrichCmd = &cobra.Command{
	Use:   "enrich",
	Short: "Enrich events with game metadata",
	Run: func(cmd *cobra.Command, args []string) {
		start := app.Start(cmd.Name(), logger)

		defer app.Catch(logger)
		defer app.Done(cmd.Name(), start, logger)

		ctxReader, cancelReader := context.WithCancel(context.Background())
		defer cancelReader()

		var wg sync.WaitGroup
		defer wg.Wait()

		topics, err := app.ParseKafkaTopicItems(viper.GetStringSlice(cmd.Name() + ".input.kafka.topic_map"))
		app.Throw("topic map parse", err)

		logger.Info("Creating kafka consumer")
		input, err := kafka.NewConsumer(&kafka.Config{
			Name:          cmd.Name() + " consumer",
			ConsumerGroup: viper.GetString(cmd.Name() + ".input.kafka.consumer_group"),
			Brokers:       viper.GetStringSlice(cmd.Name() + ".input.kafka.brokers"),
			Topics:        topics.Topics(),
			Ctx:           ctxReader,
			OffsetMode:    kafka.OffsetLastCommit,
		})
		app.Throw(cmd.Name()+" consumer", err)

		rx := input.Messages()
		tx := make(chan consumer.Message, 0)
		defer close(tx)

		chTerminate := make(chan os.Signal, 1)
		signal.Notify(chTerminate, os.Interrupt, syscall.SIGTERM)

		topicMapFn := topics.TopicMap()

		enricher, err := enrich.NewHandler(enrich.Config{})
		app.Throw("enrich handler create", err)
		defer enricher.Close()

		report := time.NewTicker(5 * time.Second)
		defer report.Stop()
	loop:
		for {
			select {
			case <-report.C:
				logger.Debugf("%+v", enricher.Counts)
			case msg, ok := <-rx:
				if !ok {
					break loop
				}
				kind, ok := topicMapFn(msg.Source)
				if !ok {
					logger.WithFields(logrus.Fields{
						"raw":    string(msg.Data),
						"source": msg.Source,
					}).Error("invalid kind")
					continue loop
				}

				event, err := enricher.Decode(msg.Data, kind)
				if err != nil {
					continue loop
				}

				if err := enricher.Enrich(event); err != nil {
					logger.WithFields(logrus.Fields{
						"raw":    string(msg.Data),
						"source": msg.Source,
						"err":    err,
					}).Error("unable to enrich")
				}

			case <-chTerminate:
				break loop
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(enrichCmd)

	app.RegisterInputKafkaCore(enrichCmd.Name(), enrichCmd.PersistentFlags())
	app.RegisterInputKafkaTopicMap(enrichCmd.Name(), enrichCmd.PersistentFlags())
}
