package cmd

import (
	"context"
	"encoding/json"
	"go-peek/internal/app"
	"go-peek/pkg/mitremeerkat"
	"go-peek/pkg/models/consumer"
	kafkaOutput "go-peek/pkg/outputs/kafka"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// mitremeerkatCmd represents the mitremeerkat command
var mitremeerkatCmd = &cobra.Command{
	Use:   "mitremeerkat",
	Short: "Push Suricata SID to MITRE ID mappings to kafka",
	Run: func(cmd *cobra.Command, args []string) {
		start := app.Start(cmd.Name(), logger)

		defer app.Catch(logger)
		defer app.Done(cmd.Name(), start, logger)

		var wg sync.WaitGroup

		csvPath := viper.GetString(cmd.Name() + ".input.file")
		logger.WithField("path", csvPath).Info("parsing csv")
		mappings, err := mitremeerkat.ParseCSV(csvPath)
		app.Throw("CSV parse", err)

		logger.WithField("items", len(mappings)).Info("csv parse done")

		tx := make(chan consumer.Message)
		defer close(tx)

		output, err := kafkaOutput.NewProducer(&kafkaOutput.Config{
			Brokers: viper.GetStringSlice(cmd.Name() + ".output.kafka.brokers"),
			Logger:  logger,
		})
		app.Throw("Sarama producer init", err)
		defer output.Close()

		topic := viper.GetString(cmd.Name() + ".output.kafka.topic")
		output.Feed(tx, "producer", context.TODO(), func(m consumer.Message) string {
			return topic
		}, &wg)

	loop:
		for _, m := range mappings {
			encoded, err := json.Marshal(m)
			if err != nil {
				logger.WithField("data", m).Error(err)
				continue loop
			}
			tx <- consumer.Message{
				Data: encoded,
				Time: time.Now(),
				Key:  "mitre_meerkat",
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(mitremeerkatCmd)

	pFlags := mitremeerkatCmd.PersistentFlags()

	pFlags.String("input-file", "", "Input file path")
	viper.BindPFlag(mitremeerkatCmd.Name()+".input.file", pFlags.Lookup("input-file"))

	app.RegisterOutputKafka(mitremeerkatCmd.Name(), mitremeerkatCmd.PersistentFlags())
}
