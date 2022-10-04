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
	"strings"
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

		var batches struct {
			syslog   int
			windows  int
			suricata int
		}

		var messages struct {
			syslog   int
			windows  int
			suricata int
		}

		normalizer := process.NewNormalizer()
		syslogCollector := &process.Collector{
			HandlerFunc: func(b *bytes.Buffer) error {
				batches.syslog++
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
							messages.syslog++
						}
					}
				}
				return scanner.Err()
			},
			Size: 64 * 1024,
		}

		windowsCollector := &process.Collector{
			HandlerFunc: func(b *bytes.Buffer) error {
				batches.windows++
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
					messages.windows++
				}
				return scanner.Err()
			},
			Size: 64 * 1024,
		}

		suricataCollector := &process.Collector{
			HandlerFunc: func(b *bytes.Buffer) error {
				batches.suricata++
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
					m := *msg.Message
					if bits := strings.SplitN(m, "LOGSTASH[-]:", 2); len(bits) == 2 {
						m = bits[1]
					}
					m = strings.TrimLeft(m, " ")
					tx <- consumer.Message{
						Data:   []byte(m),
						Event:  events.SuricataE,
						Source: events.SuricataE.String(),
					}
					messages.suricata++
				}
				return scanner.Err()
			},
		}

		logger.
			WithField("port", viper.GetInt(cmd.Name()+".input.syslog.udp.port")).
			WithField("proto", "udp").
			Info("starting up syslog server")

		report := time.NewTicker(viper.GetDuration(cmd.Name() + ".log.interval"))
		defer report.Stop()

		flush := time.NewTicker(1 * time.Second)
		defer flush.Stop()

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
					app.ErrLog(syslogCollector.Collect(msg.Data), logger)
				case val == events.EventLogE:
					app.ErrLog(windowsCollector.Collect(msg.Data), logger)
				case val == events.SuricataE:
					app.ErrLog(suricataCollector.Collect(msg.Data), logger)
				}
			case <-flush.C:
				app.ErrLog(syslogCollector.Flush(), logger)
				app.ErrLog(windowsCollector.Flush(), logger)
				app.ErrLog(suricataCollector.Flush(), logger)
			case <-chTerminate:
				break loop
			case <-report.C:
				logger.WithFields(
					logrus.Fields{
						"windows":  batches.windows,
						"syslog":   batches.syslog,
						"suricata": batches.suricata,
					},
				).Trace("batches")
				logger.WithFields(
					logrus.Fields{
						"windows":  messages.windows,
						"syslog":   messages.syslog,
						"suricata": messages.suricata,
					},
				).Debug("messages")
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
