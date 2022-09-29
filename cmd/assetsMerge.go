package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"go-peek/internal/app"
	kafkaIngest "go-peek/pkg/ingest/kafka"
	"go-peek/pkg/models"
	"go-peek/pkg/models/consumer"
	kafkaOutput "go-peek/pkg/outputs/kafka"
	"go-peek/pkg/providentia"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// assetsMergeCmd represents the assetsMerge command
var assetsMergeCmd = &cobra.Command{
	Use:   "assetsMerge",
	Short: "merge providentia and vsphere asset feeds",
	Run: func(cmd *cobra.Command, args []string) {
		start := app.Start(cmd.Name(), logger)

		defer app.Catch(logger)
		defer app.Done(cmd.Name(), start, logger)

		workdir := viper.GetString("work.dir")
		if workdir == "" {
			app.Throw("app init", errors.New("missing working directory"), logger)
		}
		workdir = path.Join(workdir, cmd.Name())
		_ = os.Mkdir(workdir, os.ModePerm)

		var wg sync.WaitGroup
		defer wg.Wait()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		logger.Info("Creating kafka consumer for asset stream")
		input, err := kafkaIngest.NewConsumer(&kafkaIngest.Config{
			Name:          cmd.Name() + " event stream",
			ConsumerGroup: viper.GetString(cmd.Name() + ".input.kafka.consumer_group"),
			Brokers:       viper.GetStringSlice(cmd.Name() + ".input.kafka.brokers"),
			Topics: []string{
				viper.GetString(cmd.Name() + ".input.kafka.topic_assets_providentia"),
				viper.GetString(cmd.Name() + ".input.kafka.topic_assets_vcenter"),
			},
			Ctx:        ctx,
			OffsetMode: kafkaOffset,
		})
		app.Throw(cmd.Name()+" asset stream setup", err, logger)

		tx := make(chan consumer.Message, 0)
		defer close(tx)

		producer, err := kafkaOutput.NewProducer(&kafkaOutput.Config{
			Brokers: viper.GetStringSlice(cmd.Name() + ".output.kafka.brokers"),
			Logger:  logger,
		})
		app.Throw("Sarama producer init", err, logger)
		producer.Feed(tx, cmd.Name()+" producer", ctx, func(m consumer.Message) string {
			return viper.GetString(cmd.Name() + ".output.kafka.topic")
		}, &wg)

		seenRecords := make(map[string]providentia.Record)
		badLookups := make(map[string]bool)

		chTerminate := make(chan os.Signal, 1)
		signal.Notify(chTerminate, os.Interrupt, syscall.SIGTERM)

		var counts struct {
			TotalVcenter         uint
			TotalProvidentia     uint
			NewAssetsFound       uint
			NewAssetsSent        uint
			MissedVcenterLookups uint
		}

		report := time.NewTicker(5 * time.Second)
		defer report.Stop()

	loop:
		for {
			select {
			case <-report.C:
				logger.Infof("%+v", counts)
				app.DumpJSON(filepath.Join(workdir, "seen_records.json"), seenRecords)
				if len(badLookups) > 0 {
					app.DumpJSON(filepath.Join(workdir, "bad_lookups.json"), badLookups)
				}
			case <-chTerminate:
				break loop
			case msg, ok := <-input.Messages():
				if !ok {
					break loop
				}
				var (
					out interface{}
					key string
				)
				switch msg.Source {
				case viper.GetString(cmd.Name() + ".input.kafka.topic_assets_providentia"):
					var obj providentia.Record
					if err := json.Unmarshal(msg.Data, &obj); err != nil {
						logger.WithFields(logrus.Fields{
							"raw":    string(msg.Data),
							"source": msg.Source,
							"err":    err,
							"kind":   "providentia",
						}).Error("unable to parse asset")
						continue loop
					}
					lookupKey := strings.TrimLeft(
						obj.AnsibleName,
						viper.GetString(cmd.Name()+".strip_prefix"),
					)
					seenRecords[lookupKey] = obj

					out = obj
					key = obj.Addr.String()

					counts.TotalProvidentia++

				case viper.GetString(cmd.Name() + ".input.kafka.topic_assets_vcenter"):
					var obj models.AssetVcenter
					if err := json.Unmarshal(msg.Data, &obj); err != nil {
						logger.WithFields(logrus.Fields{
							"raw":    string(msg.Data),
							"source": msg.Source,
							"err":    err,
							"kind":   "vcenter",
						}).Error("unable to parse asset")
						continue loop
					}
					lookupKey := strings.TrimLeft(
						obj.Name,
						viper.GetString(cmd.Name()+".strip_prefix"),
					)
					if lookupKey == "" {
						logger.
							WithField("ip", obj.IP).
							WithField("name", obj.HostName).
							WithField("raw", string(msg.Data)).
							Error("missing lookup key")
					}
					if val, ok := seenRecords[lookupKey]; ok {
						cpy := val.VsphereCopy(obj)
						out = cpy
						key = obj.IP.String()
						counts.NewAssetsFound++
						logger.
							WithField("key", lookupKey).
							WithField("pretty", cpy.Pretty).
							WithField("ip", cpy.Addr).
							Trace("vcenter mapping pickup")
						if badLookups[lookupKey] {
							delete(badLookups, lookupKey)
						}
					} else {
						counts.MissedVcenterLookups++
						logger.
							WithField("key", lookupKey).
							Warning("lookup fail")
						badLookups[lookupKey] = true
					}

					counts.TotalVcenter++
				}

				if out == nil {
					continue loop
				}

				encoded, err := json.Marshal(out)
				if err != nil {
					logger.WithFields(logrus.Fields{
						"raw":  out,
						"err":  err,
						"kind": "providentia",
					}).Error("encode json")
					continue loop
				}

				tx <- consumer.Message{
					Data: encoded,
					Key:  key,
					Time: time.Now(),
				}

				counts.NewAssetsSent++
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(assetsMergeCmd)

	assetsMergeCmd.PersistentFlags().String("strip-prefix", "", "Strip this prefix from asset key.")
	viper.BindPFlag(assetsMergeCmd.Name()+".strip_prefix", assetsMergeCmd.PersistentFlags().Lookup("strip-prefix"))

	app.RegisterInputKafkaAssetMerge(assetsMergeCmd.Name(), assetsMergeCmd.PersistentFlags())
	app.RegisterOutputKafka(assetsMergeCmd.Name(), assetsMergeCmd.PersistentFlags())
}
