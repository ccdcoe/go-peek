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

	"github.com/influxdata/go-syslog/v3/rfc5424"
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

		topics, err := app.ParseKafkaTopicItems(
			viper.GetStringSlice(cmd.Name() + ".input.kafka.topic_map"),
		)
		app.Throw("topic map parse", err, logger)

		logger.Info("Creating kafka consumer")
		input, err := kafkaIngest.NewConsumer(&kafkaIngest.Config{
			Name:          cmd.Name() + " consumer",
			ConsumerGroup: viper.GetString(cmd.Name() + ".input.kafka.consumer_group"),
			Brokers:       viper.GetStringSlice(cmd.Name() + ".input.kafka.brokers"),
			Topics:        topics.Topics(),
			Ctx:           ctxReader,
			OffsetMode:    kafkaOffset,
			Logger:        logger,
			LogInterval:   viper.GetDuration(cmd.Name() + ".log.interval"),
		})
		app.Throw("kafka consumer", err, logger)

		topicMapFn := topics.TopicMap()

		rx := input.Messages()
		tx := make(chan consumer.Message, 0)
		defer close(tx)

		ctxWriter, cancelWriter := context.WithCancel(context.Background())
		producer, err := kafkaOutput.NewProducer(&kafkaOutput.Config{
			Brokers: viper.GetStringSlice(cmd.Name() + ".output.kafka.brokers"),
			Logger:  logger,
		})
		app.Throw("Sarama producer init", err, logger)
		topic := viper.GetString(cmd.Name() + ".output.kafka.topic")
		producer.Feed(tx, cmd.Name()+" producer", ctxWriter, func(m consumer.Message) string {
			return topic + "-" + m.Source
		}, &wg)

		chTerminate := make(chan os.Signal, 1)
		signal.Notify(chTerminate, os.Interrupt, syscall.SIGTERM)

		normalizer := process.NewNormalizer()
		syslogCollector := &process.Collector{
			HandlerFunc: func(b *bytes.Buffer) error {
				logger.WithFields(logrus.Fields{
					"len": b.Len(),
				}).Trace("syslog collect handler called")

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
							var kind events.Atomic
							switch obj.(type) {
							case *events.Syslog:
								kind = events.SyslogE
							case *events.Snoopy:
								kind = events.SnoopyE
							}
							tx <- consumer.Message{
								Data:   bin,
								Event:  kind,
								Source: kind.String(),
							}
						}
					}
				}
				return scanner.Err()
			},
			Size:   64 * 1024,
			Ticker: time.NewTicker(viper.GetDuration(cmd.Name() + ".log.interval")),
		}

		windowsCollector := &process.Collector{
			HandlerFunc: func(b *bytes.Buffer) error {
				logger.WithFields(logrus.Fields{
					"len": b.Len(),
				}).Trace("windows collect handler called")

				scanner := bufio.NewScanner(b)
				for scanner.Scan() {
					// TODO - topic map per object type
					slc := make([]byte, len(scanner.Bytes()))
					copy(slc, scanner.Bytes())
					tx <- consumer.Message{
						Data:   slc,
						Event:  events.EventLogE,
						Source: events.EventLogE.String(),
					}
				}
				return scanner.Err()
			},
			Size:   64 * 1024,
			Ticker: time.NewTicker(viper.GetDuration(cmd.Name() + ".log.interval")),
		}

		suricataCollector := &process.Collector{
			HandlerFunc: func(b *bytes.Buffer) error {
				logger.WithFields(logrus.Fields{
					"len": b.Len(),
				}).Trace("suricata collect handler called")
				scanner := bufio.NewScanner(b)
			loop:
				for scanner.Scan() {
					obj, err := normalizer.RFC5424.Parse(scanner.Bytes())
					if err != nil {
						logger.WithFields(logrus.Fields{
							"msg": scanner.Text(),
							"err": err,
						}).Error("suricata syslog entry parse")
						continue loop
					}
					msg, ok := obj.(*rfc5424.SyslogMessage)
					if !ok || msg == nil || msg.Message == nil {
						logger.WithFields(logrus.Fields{
							"msg": scanner.Text(),
						}).Error("suricata message extract")
						continue loop
					}
					tx <- consumer.Message{
						Data:   []byte(*msg.Message),
						Event:  events.SuricataE,
						Source: events.SuricataE.String(),
					}
				}
				return scanner.Err()
			},
		}

		var counts struct {
			total        int
			kafkaSyslog  int
			kafkaWindows int
			suricata     int
		}

		logger.
			WithField("port", viper.GetInt(cmd.Name()+".input.syslog.udp.port")).
			WithField("proto", "udp").
			Info("starting up syslog server")

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
				switch val, ok := topicMapFn(msg.Source); ok {
				case val == events.SyslogE, val == events.SnoopyE:
					syslogCollector.Collect(msg.Data)
					counts.kafkaSyslog++
				case val == events.EventLogE:
					windowsCollector.Collect(msg.Data)
					counts.kafkaWindows++
				case val == events.SuricataE:
					suricataCollector.Collect(msg.Data)
					counts.suricata++
				}
				counts.total++
			case <-chTerminate:
				break loop
			case <-report.C:
				logger.WithFields(
					logrus.Fields{
						"total":           counts.total,
						"kafka_windows":   counts.kafkaWindows,
						"kafka_syslog":    counts.kafkaSyslog,
						"syslog_suricata": counts.suricata,
					},
				).Debug(cmd.Name())
			}
		}

		cancelReader()
		cancelWriter()
		wg.Wait()
	},
}

func init() {
	rootCmd.AddCommand(preprocessCmd)

	app.RegisterLogging(preprocessCmd.Name(), preprocessCmd.PersistentFlags())
	app.RegisterInputKafkaPreproc(preprocessCmd.Name(), preprocessCmd.PersistentFlags())
	app.RegisterOutputKafka(preprocessCmd.Name(), preprocessCmd.PersistentFlags())
}
