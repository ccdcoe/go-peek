package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"go-peek/internal/app"
	"go-peek/pkg/enrich"
	"go-peek/pkg/ingest/kafka"
	"go-peek/pkg/intel/mitre"
	"go-peek/pkg/models/consumer"
	"go-peek/pkg/models/events"
	"go-peek/pkg/persist"
	"go-peek/pkg/providentia"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	kafkaIngest "go-peek/pkg/ingest/kafka"
	kafkaOutput "go-peek/pkg/outputs/kafka"

	"github.com/markuskont/go-sigma-rule-engine/pkg/sigma/v2"
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

		// defer app.Catch(logger)
		defer app.Done(cmd.Name(), start, logger)

		ctxReader, cancelReader := context.WithCancel(context.Background())
		defer cancelReader()

		var wg sync.WaitGroup
		defer wg.Wait()
		defer func() { logger.Info("Waiting for async workers to exit") }()

		topics, err := app.ParseKafkaTopicItems(
			viper.GetStringSlice(cmd.Name() + ".input.kafka.topic_map"),
		)
		app.Throw("topic map parse", err)

		logger.Info("Creating kafka consumer for event stream")
		streamEvents, err := kafkaIngest.NewConsumer(&kafkaIngest.Config{
			Name:          cmd.Name() + " event stream",
			ConsumerGroup: viper.GetString(cmd.Name() + ".input.kafka.consumer_group"),
			Brokers:       viper.GetStringSlice(cmd.Name() + ".input.kafka.brokers"),
			Topics:        topics.Topics(),
			Ctx:           ctxReader,
			OffsetMode:    kafka.OffsetLastCommit,
		})
		app.Throw(cmd.Name()+" event stream setup", err)

		logger.Info("Creating kafka consumer for asset stream")
		streamAssets, err := kafkaIngest.NewConsumer(&kafkaIngest.Config{
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

		streamOutput, err := kafkaOutput.NewProducer(&kafkaOutput.Config{
			Brokers: viper.GetStringSlice(cmd.Name() + ".output.kafka.brokers"),
			Logger:  logger,
		})
		app.Throw("Sarama producer init", err)
		topic := viper.GetString(cmd.Name() + ".output.kafka.topic")
		streamOutput.Feed(tx, cmd.Name()+" output producer", context.TODO(),
			func(m consumer.Message) string {
				if m.Source == "emit" {
					return viper.GetString(cmd.Name() + ".output.kafka.topic_emit")
				}
				if viper.GetBool(cmd.Name() + ".output.kafka.topic_split") {
					return topic + "-" + m.Key
				}
				return topic
			}, &wg)

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

		rulesets, err := app.ParseKafkaTopicItems(viper.GetStringSlice(
			cmd.Name() + ".sigma.ruleset_path",
		))
		if err != nil && err == app.ErrInvalidTopicFlags {
			logger.Warn("no sigma rulesets found, IDS feature is disabled")
		} else {
			app.Throw("sigma ruleset init", err)
		}

		sigmaRuleMap := make(map[events.Atomic]sigma.Ruleset)
		for _, rs := range rulesets {
			logger.WithFields(logrus.Fields{
				"kind": rs.Type.String(),
				"path": rs.Topic,
			}).Debug("parsing sigma ruleset")
			ruleset, err := sigma.NewRuleset(sigma.Config{
				Directory: []string{rs.Topic},
			})
			app.Throw("sigma "+rs.Topic+" init", err)
			sigmaRuleMap[rs.Type] = *ruleset
		}

		enricher, err := enrich.NewHandler(
			enrich.Config{
				Persist: persist,
				Mitre: mitre.Config{
					EnterpriseDump: filepath.Join(workdir, "enterprise.json"),
					MappingsDump:   filepath.Join(workdir, "mappings.json"),
				},
				Sigma: sigmaRuleMap,
			},
		)
		app.Throw("enrich handler create", err)
		defer enricher.Close()

		report := time.NewTicker(viper.GetDuration(cmd.Name() + ".log.interval"))
		defer report.Stop()
	loop:
		for {
			select {
			case <-report.C:
				logger.Infof("%+v", enricher.Counts)
				if missing := enricher.MissingKeys(); len(missing) > 0 {
					logger.WithField("count", len(missing)).Warn("missing asset lookup keys")
					for _, key := range missing {
						logger.WithField("value", key).Debug("missing asset host key")
					}
				}
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
					logrus.WithFields(logrus.Fields{
						"raw":  string(msg.Data),
						"kind": kind.String(),
					}).Error("unable to decode")
					continue loop
				}

				if err := enricher.Enrich(event); err != nil {
					logger.WithFields(logrus.Fields{
						"raw":    string(msg.Data),
						"source": msg.Source,
						"err":    err,
					}).Error("unable to enrich")
					continue loop
				}

				encoded, err := event.JSONFormat()
				if err != nil {
					logger.WithFields(logrus.Fields{
						"err":   err,
						"event": event,
					}).Error("event encode error")
					continue loop
				}

				if event.Emit() {
					// mitre-enriched events should be fast-tracked
					tx <- consumer.Message{
						Data:   encoded,
						Time:   event.Time(),
						Key:    kind.String(),
						Event:  kind,
						Source: "emit",
					}
				}

				// send to generic topics
				tx <- consumer.Message{
					Data:  encoded,
					Time:  event.Time(),
					Key:   kind.String(),
					Event: kind,
				}

			case <-chTerminate:
				break loop
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(enrichCmd)

	app.RegisterLogging(enrichCmd.Name(), enrichCmd.PersistentFlags())
	app.RegisterInputKafkaCore(enrichCmd.Name(), enrichCmd.PersistentFlags())
	app.RegisterInputKafkaEnrich(enrichCmd.Name(), enrichCmd.PersistentFlags())
	app.RegisterSigmaRulesetPaths(enrichCmd.Name(), enrichCmd.PersistentFlags())
	app.RegisterOutputKafka(enrichCmd.Name(), enrichCmd.PersistentFlags())
	app.RegisterOutputKafkaEnrichment(enrichCmd.Name(), enrichCmd.PersistentFlags())
}
