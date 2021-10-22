package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"go-peek/internal/app"
	"go-peek/pkg/enrich"
	"go-peek/pkg/ingest/kafka"
	"go-peek/pkg/models/consumer"
	"go-peek/pkg/persist"
	"go-peek/pkg/providentia"
	"os"
	"os/signal"
	"path"
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
		defer func() { logger.Info("Waiting for async workers to exit") }()

		topics, err := app.ParseKafkaTopicItems(viper.GetStringSlice(cmd.Name() + ".input.kafka.topic_map"))
		app.Throw("topic map parse", err)

		logger.Info("Creating kafka consumer for event stream")
		streamEvents, err := kafka.NewConsumer(&kafka.Config{
			Name:          cmd.Name() + " event stream",
			ConsumerGroup: viper.GetString(cmd.Name() + ".input.kafka.consumer_group"),
			Brokers:       viper.GetStringSlice(cmd.Name() + ".input.kafka.brokers"),
			Topics:        topics.Topics(),
			Ctx:           ctxReader,
			OffsetMode:    kafka.OffsetLastCommit,
		})
		app.Throw(cmd.Name()+" event stream setup", err)

		logger.Info("Creating kafka consumer for asset stream")
		streamAssets, err := kafka.NewConsumer(&kafka.Config{
			Name:          cmd.Name() + " asset stream",
			ConsumerGroup: viper.GetString(cmd.Name() + ".input.kafka.consumer_group"),
			Brokers:       viper.GetStringSlice(cmd.Name() + ".input.kafka.brokers"),
			Topics:        []string{viper.GetString(cmd.Name() + ".input.kafka.topic_assets")},
			Ctx:           ctxReader,
			OffsetMode:    kafka.OffsetLastCommit,
		})
		app.Throw(cmd.Name()+" asset stream setup", err)

		tx := make(chan consumer.Message, 0)
		defer close(tx)

		chTerminate := make(chan os.Signal, 1)
		signal.Notify(chTerminate, os.Interrupt, syscall.SIGTERM)

		topicMapFn := topics.TopicMap()

		workdir := viper.GetString("work.dir")
		if workdir == "" {
			app.Throw("app init", errors.New("missing working directory"))
		}
		workdir = path.Join(workdir, cmd.Name())

		ctxPersist, cancelPersist := context.WithCancel(context.Background())
		persist, err := persist.NewBadger(persist.Config{
			Directory:     path.Join(workdir, "badger"),
			IntervalGC:    1 * time.Minute,
			RunValueLogGC: true,
			WaitGroup:     &wg,
			Ctx:           ctxPersist,
			Logger:        logger,
		})
		app.Throw("persist setup", err)
		defer persist.Close()
		defer cancelPersist()

		enricher, err := enrich.NewHandler(enrich.Config{Persist: persist})
		app.Throw("enrich handler create", err)
		defer enricher.Close()

		report := time.NewTicker(5 * time.Second)
		defer report.Stop()
	loop:
		for {
			select {
			case <-report.C:
				logger.Infof("%+v", enricher.Counts)
			case msg, ok := <-streamAssets.Messages():
				if !ok {
					continue loop
				}
				var obj providentia.Record
				if err := json.Unmarshal(msg.Data, &obj); err != nil {
					logger.WithFields(logrus.Fields{
						"raw":    string(msg.Data),
						"source": msg.Source,
						"err":    err,
					}).Error("unable to parse asset")
					continue loop
				}
				enricher.AddAsset(obj)
			case msg, ok := <-streamEvents.Messages():
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
	app.RegisterInputKafkaEnrich(enrichCmd.Name(), enrichCmd.PersistentFlags())
}
