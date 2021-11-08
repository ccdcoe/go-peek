package cmd

import (
	"context"
	"encoding/json"
	"go-peek/internal/app"
	"go-peek/pkg/ingest/kafka"
	kafkaIngest "go-peek/pkg/ingest/kafka"
	"go-peek/pkg/models"
	"go-peek/pkg/models/consumer"
	kafkaOutput "go-peek/pkg/outputs/kafka"
	"go-peek/pkg/providentia"
	"os"
	"os/signal"
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

		var wg sync.WaitGroup
		defer wg.Wait()

		ctxReader, ctxCancel := context.WithCancel(context.Background())
		logger.Info("Creating kafka consumer for asset stream")
		input, err := kafkaIngest.NewConsumer(&kafkaIngest.Config{
			Name:          cmd.Name() + " event stream",
			ConsumerGroup: viper.GetString(cmd.Name() + ".input.kafka.consumer_group"),
			Brokers:       viper.GetStringSlice(cmd.Name() + ".input.kafka.brokers"),
			Topics: []string{
				viper.GetString(cmd.Name() + ".input.kafka.topic_assets_providentia"),
				viper.GetString(cmd.Name() + ".input.kafka.topic_assets_vcenter"),
			},
			Ctx:        ctxReader,
			OffsetMode: kafka.OffsetLastCommit,
		})
		app.Throw(cmd.Name()+" asset stream setup", err)
		defer ctxCancel()

		tx := make(chan consumer.Message, 0)
		defer close(tx)

		ctxWriter, cancelWriter := context.WithCancel(context.Background())
		producer, err := kafkaOutput.NewProducer(&kafkaOutput.Config{
			Brokers: viper.GetStringSlice(cmd.Name() + ".output.kafka.brokers"),
			Logger:  logger,
		})
		app.Throw("Sarama producer init", err)
		producer.Feed(tx, cmd.Name()+" producer", ctxWriter, func(m consumer.Message) string {
			return viper.GetString(cmd.Name() + ".output.kafka.topic")
		}, &wg)
		defer cancelWriter()

		seenAddrs := make(map[string]bool)
		seenRecords := make(map[string]providentia.Record)

		chTerminate := make(chan os.Signal, 1)
		signal.Notify(chTerminate, os.Interrupt, syscall.SIGTERM)

		var counts struct {
			TotalVcenter     uint
			TotalProvidentia uint
			NewAssetsFound   uint
			NewAssetsSent    uint
		}

		report := time.NewTicker(5 * time.Second)
		defer report.Stop()

	loop:
		for {
			select {
			case <-report.C:
				logger.Infof("%+v", counts)
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
					seenAddrs[obj.Addr.String()] = true
					seenRecords[obj.AnsibleName] = obj

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
					if len(seenRecords) > 0 && !seenAddrs[obj.IP.String()] {
						if val, ok := seenRecords[obj.AnsibleName]; ok {
							out = val.VsphereCopy(obj)
							key = obj.IP.String()
							seenAddrs[obj.IP.String()] = true
							counts.NewAssetsFound++
						}
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

	app.RegisterInputKafkaAssetMerge(assetsMergeCmd.Name(), assetsMergeCmd.PersistentFlags())
	app.RegisterOutputKafka(assetsMergeCmd.Name(), assetsMergeCmd.PersistentFlags())
}
