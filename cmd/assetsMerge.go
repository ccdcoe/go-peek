package cmd

import (
	"context"
	"encoding/json"
	"go-peek/internal/app"
	"go-peek/pkg/ingest/kafka"
	kafkaIngest "go-peek/pkg/ingest/kafka"
	"go-peek/pkg/models"
	"go-peek/pkg/providentia"

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
			OffsetMode: kafka.OffsetEarliest,
		})
		app.Throw(cmd.Name()+" asset stream setup", err)
		defer ctxCancel()

	loop:
		for {
			select {
			case msg, ok := <-input.Messages():
				if !ok {
					break loop
				}
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

				}
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(assetsMergeCmd)

	app.RegisterInputKafkaAssetMerge(assetsMergeCmd.Name(), assetsMergeCmd.PersistentFlags())
	app.RegisterOutputKafka(assetsMergeCmd.Name(), assetsMergeCmd.PersistentFlags())
}
