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
		start := app.Start(cmd.Name(), logger)

		defer app.Catch(logger)
		defer app.Done(cmd.Name(), start, logger)

		ctxReader, cancelReader := context.WithCancel(context.Background())

		var wg sync.WaitGroup

		logger.Info("Creating kafka consumer")
		input, err := kafka.NewConsumer(&kafka.Config{
			Name:          cmd.Name() + " consumer",
			ConsumerGroup: viper.GetString(cmd.Name() + ".input.kafka.consumer_group"),
			Brokers:       viper.GetStringSlice(cmd.Name() + ".input.kafka.brokers"),
			Topics:        viper.GetStringSlice(cmd.Name() + ".input.kafka.topics"),
			Ctx:           ctxReader,
			OffsetMode:    kafkaOffset,
			Logger:        logger,
			LogInterval:   viper.GetDuration(cmd.Name() + ".log.interval"),
		})
		app.Throw("kafka consumer", err, logger)

		rx := input.Messages()
		tx := make(chan consumer.Message, 0)
		defer close(tx)

		writer, err := elastic.NewHandle(&elastic.Config{
			Workers:  4,
			Debug:    false,
			Hosts:    viper.GetStringSlice(cmd.Name() + ".output.elasticsearch.hosts"),
			Interval: elastic.DefaultBulkFlushInterval,
			Stream:   tx,
			Logger:   logger,
			Username: viper.GetString(cmd.Name() + ".output.elasticsearch.xpack.user"),
			Password: viper.GetString(cmd.Name() + ".output.elasticsearch.xpack.pass"),
			Fn: func(m consumer.Message) string {
				prefix := viper.GetString(cmd.Name() + ".output.elasticsearch.prefix")
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
		app.Throw(cmd.Name()+" output create", err, logger)
		defer writer.Close()

		logrus.Debug("starting up writer")
		ctxWriter, cancelWriter := context.WithCancel(context.Background())
		app.Throw("writer routine create", writer.Do(ctxWriter, &wg), logger)

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

	app.RegisterLogging(elasticCmd.Name(), elasticCmd.PersistentFlags())
	app.RegisterInputKafkaGenericSimple(elasticCmd.Name(), elasticCmd.PersistentFlags())
	app.RegisterOutputElastic(elasticCmd.Name(), elasticCmd.PersistentFlags())
}
