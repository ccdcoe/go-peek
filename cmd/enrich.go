package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"go-peek/internal/app"
	"go-peek/pkg/enrich"
	"go-peek/pkg/intel/mitre"
	"go-peek/pkg/mitremeerkat"
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

	"github.com/markuskont/go-sigma-rule-engine"
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

		topics, err := app.ParseKafkaTopicItems(
			viper.GetStringSlice(cmd.Name() + ".input.kafka.topic_map"),
		)
		app.Throw("topic map parse", err, logger)

		logger.Info("Creating kafka consumer for event stream")
		streamEvents, err := kafkaIngest.NewConsumer(&kafkaIngest.Config{
			Name:          cmd.Name() + " event stream",
			ConsumerGroup: viper.GetString(cmd.Name() + ".input.kafka.consumer_group"),
			Brokers:       viper.GetStringSlice(cmd.Name() + ".input.kafka.brokers"),
			Topics:        topics.Topics(),
			Ctx:           ctxReader,
			OffsetMode:    kafkaOffset,
		})
		app.Throw(cmd.Name()+" event stream setup", err, logger)

		logger.Info("Creating kafka consumer for asset stream")
		streamAssets, err := kafkaIngest.NewConsumer(&kafkaIngest.Config{
			Name:          cmd.Name() + " asset stream",
			ConsumerGroup: viper.GetString(cmd.Name() + ".input.kafka.consumer_group"),
			Brokers:       viper.GetStringSlice(cmd.Name() + ".input.kafka.brokers"),
			Topics:        []string{viper.GetString(cmd.Name() + ".input.kafka.topic_assets")},
			Ctx:           ctxReader,
			OffsetMode:    kafkaOffset,
			Logger:        logger,
		})
		app.Throw(cmd.Name()+" asset stream setup", err, logger)

		tx := make(chan consumer.Message, 0)
		defer close(tx)

		streamOutput, err := kafkaOutput.NewProducer(&kafkaOutput.Config{
			Brokers: viper.GetStringSlice(cmd.Name() + ".output.kafka.brokers"),
			Logger:  logger,
		})
		app.Throw("Sarama producer init", err, logger)
		topic := viper.GetString(cmd.Name() + ".output.kafka.topic")
		streamOutput.Feed(tx, cmd.Name()+" output producer", context.TODO(),
			func(m consumer.Message) string {
				if m.Source == "oracle" {
					return viper.GetString(cmd.Name() + ".output.kafka.topic_oracle")
				}
				if m.Source == "emit" {
					return viper.GetString(cmd.Name() + ".output.kafka.topic_emit")
				}
				if viper.GetBool(cmd.Name() + ".output.kafka.topic_split") {
					return topic + "-" + m.Source
				}
				return topic
			}, &wg)

		chTerminate := make(chan os.Signal, 1)
		signal.Notify(chTerminate, os.Interrupt, syscall.SIGTERM)

		topicMapFn := topics.TopicMap()

		workdir := viper.GetString("work.dir")
		if workdir == "" {
			app.Throw("app init", errors.New("missing working directory"), logger)
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
		app.Throw("persist setup", err, logger)
		defer persist.Close()
		defer cancelPersist()

		rulesets, err := app.ParseKafkaTopicItems(viper.GetStringSlice(
			cmd.Name() + ".sigma.ruleset_path",
		))
		if err != nil && err == app.ErrInvalidTopicFlags {
			logger.Warn("no sigma rulesets found, IDS feature is disabled")
		} else {
			app.Throw("sigma ruleset init", err, logger)
		}

		sigmaRuleMap := make(map[events.Atomic]sigma.Ruleset)
		for _, rs := range rulesets {
			logger.WithFields(logrus.Fields{
				"kind": rs.Type.String(),
				"path": rs.Topic,
			}).Trace("parsing sigma ruleset")
			ruleset, err := sigma.NewRuleset(sigma.Config{
				Directory: []string{rs.Topic},
			})
			app.Throw("sigma "+rs.Topic+" init", err, logger)
			sigmaRuleMap[rs.Type] = *ruleset
			logger.WithFields(logrus.Fields{
				"path":         rs.Topic,
				"type":         rs.Type.String(),
				"sigma_parsed": ruleset.Ok,
				"sigma_failed": ruleset.Failed,
				"sigma_unsupp": ruleset.Unsupported,
				"sigma_total":  ruleset.Total,
			}).Debug("ruleset parsed")
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
		app.Throw("enrich handler create", err, logger)
		defer func() {
			if err := enricher.Close(); err != nil {
				logger.WithField("err", err).Error("problem closing enricher")
			}
		}()

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
				if sidMap := enricher.MissingSidMaps(); len(sidMap) > 0 {
					logger.WithField("count", len(sidMap)).Warn("missing suricata MITRE SID mappings")
					for sid, msg := range sidMap {
						logger.WithFields(logrus.Fields{
							"sid": sid,
							"msg": msg,
						}).Debug("missing MITRE SID mapping")
					}
					mappings := mitremeerkat.NewMappings(sidMap)
					encoded, err := json.Marshal(mappings)
					if err != nil {
						logger.WithFields(logrus.Fields{
							"raw": mappings,
							"err": err,
						}).Error("unable to encode json")
						continue loop
					}
					tx <- consumer.Message{
						Source: "oracle",
						Key:    "meerkat_missing_sid_map",
						Data:   encoded,
					}
				}
				enricher.Persist()
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
						"err":  err.Error(),
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
					if viper.GetBool(cmd.Name() + ".stdout.emit") {
						os.Stdout.Write(append(encoded, []byte("\n")...))
					}
					if !viper.GetBool(cmd.Name() + ".noproduce") {
						// mitre-enriched events should be fast-tracked
						tx <- consumer.Message{
							Data:   encoded,
							Time:   event.Time(),
							Key:    kind.String(),
							Event:  kind,
							Source: "emit",
						}
					}
				}

				if viper.GetBool(cmd.Name() + ".stdout.events") {
					os.Stdout.Write(append(encoded, []byte("\n")...))
				}

				if !viper.GetBool(cmd.Name() + ".noproduce") {
					// send to generic topics
					tx <- consumer.Message{
						Data:   encoded,
						Time:   event.Time(),
						Event:  kind,
						Source: kind.String(),
					}
				}

			case <-chTerminate:
				break loop
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(enrichCmd)

	pFlags := enrichCmd.PersistentFlags()

	pFlags.Bool("stdout-events", false, "Write results to stdout. Mostly for debug.")
	viper.BindPFlag(enrichCmd.Name()+".stdout.events", pFlags.Lookup("stdout-events"))

	pFlags.Bool("stdout-emit", false, "Write results to stdout. Mostly for debug.")
	viper.BindPFlag(enrichCmd.Name()+".stdout.emit", pFlags.Lookup("stdout-emit"))

	pFlags.Bool("no-produce", false, "Do not write to kafka. Mostly for debug.")
	viper.BindPFlag(enrichCmd.Name()+".noproduce", pFlags.Lookup("no-produce"))

	app.RegisterLogging(enrichCmd.Name(), enrichCmd.PersistentFlags())
	app.RegisterInputKafkaCore(enrichCmd.Name(), enrichCmd.PersistentFlags())
	app.RegisterInputKafkaTopicMap(enrichCmd.Name(), enrichCmd.PersistentFlags())
	app.RegisterInputKafkaEnrich(enrichCmd.Name(), enrichCmd.PersistentFlags())
	app.RegisterSigmaRulesetPaths(enrichCmd.Name(), enrichCmd.PersistentFlags())
	app.RegisterOutputKafka(enrichCmd.Name(), enrichCmd.PersistentFlags())
	app.RegisterOutputKafkaEnrichment(enrichCmd.Name(), enrichCmd.PersistentFlags())
	app.RegisterOutputKafkaOracle(enrichCmd.Name(), enrichCmd.PersistentFlags())
}
