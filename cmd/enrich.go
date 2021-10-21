package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"go-peek/internal/app"
	"go-peek/pkg/ingest/kafka"
	"go-peek/pkg/models/consumer"
	"go-peek/pkg/models/events"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// enrichCmd represents the enrich command
var enrichCmd = &cobra.Command{
	Use:   "enrich",
	Short: "Enrich events with game metadata",
	Run: func(cmd *cobra.Command, args []string) {
		start := app.Start(cmd.Name(), logger)

		defer app.Catch(logger)
		defer app.Done(cmd.Name(), start, logger)

		ctxReader, cancelReader := context.WithCancel(context.Background())
		defer cancelReader()

		var wg sync.WaitGroup
		defer wg.Wait()

		topics, err := app.ParseKafkaTopicItems(viper.GetStringSlice(cmd.Name() + ".input.kafka.topic_map"))
		app.Throw("topic map parse", err)

		logger.Info("Creating kafka consumer")
		input, err := kafka.NewConsumer(&kafka.Config{
			Name:          cmd.Name() + " consumer",
			ConsumerGroup: viper.GetString(cmd.Name() + ".input.kafka.consumer_group"),
			Brokers:       viper.GetStringSlice(cmd.Name() + ".input.kafka.brokers"),
			Topics:        topics.Topics(),
			Ctx:           ctxReader,
			OffsetMode:    kafka.OffsetLastCommit,
		})
		app.Throw(cmd.Name()+" consumer", err)

		rx := input.Messages()
		tx := make(chan consumer.Message, 0)
		defer close(tx)

		chTerminate := make(chan os.Signal, 1)
		signal.Notify(chTerminate, os.Interrupt, syscall.SIGTERM)

		topicMapFn := topics.TopicMap()

		var counts struct {
			events             uint
			invalidKind        uint
			suricataParseError uint
			windowsParseError  uint
			syslogParseError   uint
			snoopyParseError   uint
		}
		report := time.NewTicker(5 * time.Second)
		defer report.Stop()
	loop:
		for {
			select {
			case <-report.C:
				logger.Debugf("%+v", counts)
			case msg, ok := <-rx:
				if !ok {
					break loop
				}
				kind, ok := topicMapFn(msg.Source)
				if !ok {
					counts.invalidKind++
					continue loop
				}
				counts.events++

				var event events.GameEvent

				switch kind {
				case events.SuricataE:
					var obj events.Suricata
					if err := json.Unmarshal(msg.Data, &obj); err != nil {
						counts.suricataParseError++
						continue loop
					}
					event = &obj
				case events.EventLogE, events.SysmonE:
					var obj events.DynamicWinlogbeat
					if err := json.Unmarshal(msg.Data, &obj); err != nil {
						counts.windowsParseError++
						continue loop
					}
					event = &obj
				case events.SyslogE:
					var obj events.Syslog
					if err := json.Unmarshal(msg.Data, &obj); err != nil {
						counts.syslogParseError++
						continue loop
					}
					event = &obj
				case events.SnoopyE:
					var obj events.Snoopy
					if err := json.Unmarshal(msg.Data, &obj); err != nil {
						counts.snoopyParseError++
						continue loop
					}
					event = &obj
				}

				fmt.Printf("%+v \n", event)

			case <-chTerminate:
				break loop
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(enrichCmd)

	app.RegisterInputKafkaCore(enrichCmd.Name(), enrichCmd.PersistentFlags())
	app.RegisterInputKafkaTopicMap(enrichCmd.Name(), enrichCmd.PersistentFlags())
}
