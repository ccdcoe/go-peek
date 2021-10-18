package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"go-peek/internal/app"
	"go-peek/pkg/models/consumer"
	"go-peek/pkg/models/events"
	"go-peek/pkg/process"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	kafkaIngest "go-peek/pkg/ingest/kafka"
	kafkaOutput "go-peek/pkg/outputs/kafka"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// preprocessCmd represents the preprocess command
var preprocessCmd = &cobra.Command{
	Use:   "preprocess",
	Short: "Preprocess and normalize messages",
	Run: func(cmd *cobra.Command, args []string) {
		start := time.Now()
		logger.WithFields(logrus.Fields{}).Info("preprocess called")

		defer logger.WithFields(logrus.Fields{"duration": time.Since(start)}).Info("All done!")
		defer app.Catch(logger)

		ctxReader, cancelReader := context.WithCancel(context.Background())

		var wg sync.WaitGroup

		logger.Info("Creating kafka consumer")
		input, err := kafkaIngest.NewConsumer(&kafkaIngest.Config{
			Name:          "preprocess consumer",
			ConsumerGroup: viper.GetString("preprocess.input.kafka.consumer_group"),
			Brokers:       viper.GetStringSlice("preprocess.input.kafka.brokers"),
			Topics:        viper.GetStringSlice("preprocess.input.kafka.topics"),
			Ctx:           ctxReader,
			OffsetMode:    kafkaIngest.OffsetLastCommit,
		})
		app.Throw("kafka consumer", err)

		rx := input.Messages()
		tx := make(chan consumer.Message, 0)
		defer close(tx)

		ctxWriter, cancelWriter := context.WithCancel(context.Background())
		producer, err := kafkaOutput.NewProducer(&kafkaOutput.Config{
			Brokers: viper.GetStringSlice("preprocess.output.kafka.brokers"),
			Logger:  logger,
		})
		app.Throw("Sarama producer init", err)
		topic := viper.GetString("preprocess.output.kafka.topic")
		producer.Feed(tx, "Preprocess producer", ctxWriter, func(m consumer.Message) string {
			if m.Key != "" {
				return topic + "-" + m.Key
			}
			return topic
		}, &wg)

		chTerminate := make(chan os.Signal, 1)
		signal.Notify(chTerminate, os.Interrupt, syscall.SIGTERM)

		normalizer := process.NewNormalizer()
		collector := &process.Collector{
			HandlerFunc: func(b *bytes.Buffer) error {
				logger.WithFields(logrus.Fields{
					"len": b.Len(),
				}).Trace("collect handler called")

				scanner := bufio.NewScanner(b)
				for scanner.Scan() {
					obj, err := normalizer.NormalizeSyslog(scanner.Bytes())
					if err != nil && err != io.EOF {
						logger.Error(err)
					} else if err == nil {
						bin, err := json.Marshal(obj)
						if err != nil {
							logger.Error(err)
						} else {
							var key string
							switch obj.(type) {
							case *events.Syslog:
								key = "syslog"
							case *events.Snoopy:
								key = "snoopy"
							}
							// TODO - topic map per object type
							tx <- consumer.Message{
								Data: bin,
								Key:  key,
							}
						}
					}
				}
				return scanner.Err()
			},
			Size:   64 * 1024,
			Ticker: time.NewTicker(1 * time.Second),
		}

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
				collector.Collect(msg.Data)
				count++
			case <-chTerminate:
				break loop
			case <-report.C:
				logger.WithFields(logrus.Fields{"normalized": count}).Debug("preprocess")
			}
		}

		cancelReader()
		cancelWriter()
		wg.Wait()
	},
}

func init() {
	rootCmd.AddCommand(preprocessCmd)

	app.RegisterInputKafkaGenericSimple("preprocess", preprocessCmd.PersistentFlags())
	app.RegisterOutputKafka("preprocess", preprocessCmd.PersistentFlags())
}
