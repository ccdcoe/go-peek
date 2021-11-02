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
		start := app.Start(cmd.Name(), logger)

		defer app.Catch(logger)
		defer app.Done(cmd.Name(), start, logger)

		ctxReader, cancelReader := context.WithCancel(context.Background())

		var wg sync.WaitGroup

		logger.Info("Creating kafka consumer")
		input, err := kafkaIngest.NewConsumer(&kafkaIngest.Config{
			Name:          cmd.Name() + " consumer",
			ConsumerGroup: viper.GetString(cmd.Name() + ".input.kafka.consumer_group"),
			Brokers:       viper.GetStringSlice(cmd.Name() + ".input.kafka.brokers"),
			Topics:        viper.GetStringSlice(cmd.Name() + ".input.kafka.topics"),
			Ctx:           ctxReader,
			OffsetMode:    kafkaIngest.OffsetLastCommit,
		})
		app.Throw("kafka consumer", err)

		rx := input.Messages()
		tx := make(chan consumer.Message, 0)
		defer close(tx)

		ctxWriter, cancelWriter := context.WithCancel(context.Background())
		producer, err := kafkaOutput.NewProducer(&kafkaOutput.Config{
			Brokers: viper.GetStringSlice(cmd.Name() + ".output.kafka.brokers"),
			Logger:  logger,
		})
		app.Throw("Sarama producer init", err)
		topic := viper.GetString(cmd.Name() + ".output.kafka.topic")
		producer.Feed(tx, cmd.Name()+" producer", ctxWriter, func(m consumer.Message) string {
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
							var kind events.Atomic
							switch obj.(type) {
							case *events.Syslog:
								key = "syslog"
								kind = events.SyslogE
							case *events.Snoopy:
								key = "snoopy"
								kind = events.SnoopyE
							}
							// TODO - topic map per object type
							tx <- consumer.Message{
								Data:  bin,
								Key:   key,
								Event: kind,
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
				logger.WithFields(logrus.Fields{"normalized": count}).Debug(cmd.Name())
			}
		}

		cancelReader()
		cancelWriter()
		wg.Wait()
	},
}

func init() {
	rootCmd.AddCommand(preprocessCmd)

	app.RegisterInputKafkaGenericSimple(preprocessCmd.Name(), preprocessCmd.PersistentFlags())
	app.RegisterOutputKafka(preprocessCmd.Name(), preprocessCmd.PersistentFlags())
}
