package run

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	inKafka "github.com/ccdcoe/go-peek/pkg/ingest/kafka"
	"github.com/ccdcoe/go-peek/pkg/models/events"
	"github.com/ccdcoe/go-peek/pkg/utils"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	log "github.com/sirupsen/logrus"
)

var (
	Workers = 1
)

func Entrypoint(cmd *cobra.Command, args []string) {
	Workers = viper.GetInt("work.threads")
	spooldir, err := utils.ExpandHome(viper.GetString("work.dir"))
	if err != nil {
		log.Fatal(err)
	}

	log.WithFields(log.Fields{
		"workers": Workers,
		"dir":     spooldir,
	}).Debug("config parameter debug")

	if !viper.GetBool("input.kafka.enabled") {
		log.Fatal("no inputs")
	}

	// Kafka start
	kafkaCtx, kafkaCancel := context.WithCancel(context.Background())
	kafkaConsumer, err := inKafka.NewConsumer(&inKafka.Config{
		Brokers:       viper.GetStringSlice("input.kafka.host"),
		ConsumerGroup: viper.GetString("input.kafka.group"),
		Ctx:           kafkaCtx,
		Topics: func() []string {
			topics := []string{}
			for _, event := range events.Atomics {
				var src []string
				if src = viper.GetStringSlice(
					fmt.Sprintf("stream.%s.kafka.topic", event.String()),
				); len(src) == 0 {
					log.WithFields(log.Fields{
						"type":   event.String(),
						"source": "kafka",
					}).Trace("input not configured")
					continue
				}
				topics = append(topics, src...)
			}
			return topics
		}(),
		OffsetMode: func() inKafka.OffsetMode {
			switch viper.GetString("input.kafka.mode") {
			case "beginning":
				return inKafka.OffsetEarliest
			case "latest":
				return inKafka.OffsetLatest
			default:
				return inKafka.OffsetLastCommit
			}
		}(),
	})
	if err != nil {
		log.WithFields(log.Fields{
			"action": "input spawn",
			"module": "kafka consumer",
			"hosts":  viper.GetStringSlice("input.kafka.host"),
		}).Fatal(err)
	}
	// handle ctrl-c exit
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		kafkaCancel()
	}()
	modified, modWorkerErrors := spawnWorkers(
		kafkaConsumer.Messages(),
		Workers,
		spooldir,
		func(topic string) events.Atomic {
			mapping := func() map[string]events.Atomic {
				out := make(map[string]events.Atomic)
				for _, event := range events.Atomics {
					if src := viper.GetStringSlice(
						fmt.Sprintf("stream.%s.kafka.topic", event.String()),
					); len(src) > 0 {
						for _, item := range src {
							out[item] = event
						}
					}
				}
				return out
			}()
			if val, ok := mapping[topic]; ok {
				return val
			}
			return events.SimpleE
		},
	)

	go func() {
		for err := range modWorkerErrors.Items {
			log.Error(err)
		}
	}()

	for msg := range modified {
		//fmt.Fprintf(os.Stdout, ">>>%s:%d:%d:%s\n", msg.Source, msg.Partition, msg.Offset, msg.Key)
		msg.Data = make([]byte, 0)
	}
}
