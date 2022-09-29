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
			OffsetMode:    kafkaIngest.OffsetLastCommit,
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
			if m.Key != "" {
				return topic + "-" + m.Key
			}
			return topic
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
							var key string
							var kind events.Atomic
							switch obj.(type) {
							case *events.Syslog:
								kind = events.SyslogE
								key = kind.String()
							case *events.Snoopy:
								kind = events.SnoopyE
								key = kind.String()
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
						Data:  slc,
						Key:   events.EventLogE.String(),
						Event: events.EventLogE,
					}
				}
				return scanner.Err()
			},
			Size:   64 * 1024,
			Ticker: time.NewTicker(viper.GetDuration(cmd.Name() + ".log.interval")),
		}

		var counts struct {
			total          int
			kafkaSyslog    int
			kafkaWindows   int
			syslogSuricata int
		}

		logger.
			WithField("port", viper.GetInt(cmd.Name()+".input.syslog.udp.port")).
			WithField("proto", "udp").
			Info("starting up syslog server")

		udpSyslogSuricata, err := process.NewSyslogServer(func(b *bytes.Buffer) error {
			logger.WithFields(logrus.Fields{
				"len": b.Len(),
			}).Trace("udp syslog collect handler called")
			scanner := bufio.NewScanner(b)
		loop:
			for scanner.Scan() {
				// FIXME - refactor hardcoded logstash cutset
				bits := bytes.SplitN(scanner.Bytes(), []byte("LOGSTASH[-]: "), 2)
				if bits == nil && len(bits) != 2 {
					logger.WithFields(logrus.Fields{
						"msg": scanner.Text(),
					}).Error("udp syslog msg split")
					continue loop
				}
				slc := make([]byte, len(bits[1]))
				copy(slc, bits[1])
				key := events.SuricataE.String()
				if process.IsSuricataAlert(slc) {
					key += "-alert"
				}
				tx <- consumer.Message{
					Data:  slc,
					Event: events.SuricataE,
					Key:   key,
				}
				counts.syslogSuricata++
			}
			return scanner.Err()
		}, viper.GetInt(cmd.Name()+".input.syslog.udp.port"))
		app.Throw("udp syslog setup", err, logger)

		ctxUDPSyslog, cancelUDPSyslog := context.WithCancel(context.Background())
		app.Throw("udp syslog worker spawn", udpSyslogSuricata.Run(&wg, ctxUDPSyslog), logger)

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
				}
				counts.total++
			case err, ok := <-udpSyslogSuricata.Errors:
				if ok && err != nil {
					logger.Error(err)
				}
			case <-chTerminate:
				break loop
			case <-report.C:
				logger.WithFields(
					logrus.Fields{
						"total":           counts.total,
						"kafka_windows":   counts.kafkaWindows,
						"kafka_syslog":    counts.kafkaSyslog,
						"syslog_suricata": counts.syslogSuricata,
					},
				).Debug(cmd.Name())
			}
		}

		cancelReader()
		cancelWriter()
		cancelUDPSyslog()
		wg.Wait()
	},
}

func init() {
	rootCmd.AddCommand(preprocessCmd)

	app.RegisterLogging(preprocessCmd.Name(), preprocessCmd.PersistentFlags())
	app.RegisterInputKafkaPreproc(preprocessCmd.Name(), preprocessCmd.PersistentFlags())
	app.RegisterOutputKafka(preprocessCmd.Name(), preprocessCmd.PersistentFlags())
	app.RegisterInputSyslogUDP(preprocessCmd.Name(), preprocessCmd.PersistentFlags())
}
