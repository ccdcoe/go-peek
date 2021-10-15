package cmd

import (
	"context"
	"fmt"
	"go-peek/internal/app"
	"go-peek/pkg/ingest/kafka"
	"go-peek/pkg/models/consumer"
	"go-peek/pkg/outputs/elastic"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// elasticCmd represents the elastic command
var elasticCmd = &cobra.Command{
	Use:   "elastic",
	Short: "Consume messages from kafka and ship to elastic",
	Run: func(cmd *cobra.Command, args []string) {
		start := time.Now()
		logger.WithFields(logrus.Fields{}).Info("elastic called")

		defer logger.WithFields(logrus.Fields{"duration": time.Since(start)}).Info("All done!")
		defer app.Catch(logger)

		ctxReader, cancelReader := context.WithCancel(context.Background())

		var wg sync.WaitGroup

		logger.Info("Creating kafka consumer")
		input, err := kafka.NewConsumer(&kafka.Config{
			Name:          "elastic consumer",
			ConsumerGroup: "peek",
			Brokers:       viper.GetStringSlice("elastic.input.kafka.brokers"),
			Topics:        viper.GetStringSlice("elastic.input.kafka.topics"),
			Ctx:           ctxReader,
			OffsetMode:    kafka.OffsetLastCommit,
		})
		app.Throw("kafka consumer", err)

		rx := input.Messages()
		tx := make(chan consumer.Message, 0)
		defer close(tx)

		writer, err := elastic.NewHandle(&elastic.Config{
			Workers:  4,
			Debug:    false,
			Hosts:    viper.GetStringSlice("elastic.output.elasticsearch.hosts"),
			Interval: elastic.DefaultBulkFlushInterval,
			Stream:   tx,
			Logger:   logger,
			Fn: func(m consumer.Message) string {
				prefix := viper.GetString("elastic.output.elasticsearch.prefix")
				if prefix == "" {
					prefix = "peek"
				}
				topic := m.Source
				topic = strings.ReplaceAll(topic, " ", "")
				topic = strings.ReplaceAll(topic, ".", "-")
				topic = strings.ReplaceAll(topic, "_", "-")
				timestamp := m.Time
				if timestamp.IsZero() {
					timestamp = time.Now()
				}
				return fmt.Sprintf("%s-%s-%s", prefix, topic, timestamp.Format(elastic.TimeFmt))
			},
		})
		app.Throw("elastic output create", err)

		logrus.Debug("starting up writer")
		ctxWriter, cancelWriter := context.WithCancel(context.Background())
		app.Throw("writer routine create", writer.Do(ctxWriter, &wg))

		chTerminate := make(chan os.Signal, 1)
		signal.Notify(chTerminate, os.Interrupt, syscall.SIGTERM)

		var count int
		report := time.NewTicker(15 * time.Second)
		defer report.Stop()
		logger.Info("Starting main loop")
	loop:
		for {
			select {
			case msg, ok := <-rx:
				if !ok {
					break loop
				}
				tx <- *msg
				count++
			case <-chTerminate:
				break loop
			case <-report.C:
				logger.WithFields(logrus.Fields{"forwarded": count}).Debug("elastic bulk")
			}
		}
		cancelReader()
		cancelWriter()
		wg.Wait()
	},
}

func init() {
	rootCmd.AddCommand(elasticCmd)

	app.RegisterInputKafkaGenericSimple("elastic", elasticCmd.PersistentFlags())
	app.RegisterOutputElastic("elastic", elasticCmd.PersistentFlags())
}
