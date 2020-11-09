package inputs

import (
	"context"
	"fmt"

	"go-peek/internal/helpers"
	"go-peek/pkg/ingest/kafka"
	"go-peek/pkg/ingest/logfile"
	"go-peek/pkg/ingest/uxsock"
	"go-peek/pkg/models/consumer"
	"go-peek/pkg/models/events"

	"github.com/spf13/viper"

	log "github.com/sirupsen/logrus"
)

func Create(workers int, spooldir string) ([]consumer.Messager, []context.CancelFunc) {
	log.WithFields(log.Fields{
		"workers": workers,
		"dir":     spooldir,
	}).Debug("config parameter debug")

	if !viper.GetBool("input.kafka.enabled") &&
		!viper.GetBool("input.dir.enabled") &&
		!viper.GetBool("input.uxsock.enabled") {
		log.Fatal("no inputs")
	}
	inputs := make([]consumer.Messager, 0)
	stoppers := make([]context.CancelFunc, 0)

	if viper.GetBool("input.dir.enabled") {
		ctx, cancel := context.WithCancel(context.Background())
		files := helpers.GetDirListingFromViper()
		consumer, err := logfile.NewConsumer(&logfile.Config{
			Paths:       files.Files(),
			StatWorkers: viper.GetInt("work.threads"),
			ConsumeWorkers: func() int {
				// 2-3 IO readers can easily saturate most workers, unless no actual processing happens before shipping
				if !viper.GetBool("processor.enabled") {
					return viper.GetInt("work.threads")
				}
				return 3
			}(),
			MapFunc: files.MapFunc(),
			Ctx:     ctx,
		})
		if err != nil {
			log.Fatal(err)
		}
		fileListing := consumer.GetFileListing()
		for _, f := range fileListing {
			log.WithFields(log.Fields{
				"fn":   "file input create",
				"file": f,
			}).Trace()
		}
		inputs = append(inputs, consumer)
		stoppers = append(stoppers, cancel)
	}
	if viper.GetBool("input.uxsock.enabled") {
		ctx, cancel := context.WithCancel(context.Background())
		files := helpers.GetUxSockistingFromViper()
		consumer, err := uxsock.NewConsumer(&uxsock.Config{
			Ctx:     ctx,
			Sockets: files.Files(),
			MapFunc: files.MapFunc(),
			Force:   viper.GetBool("input.uxsock.overwrite"),
		})
		if err != nil {
			log.Fatal(err)
		}
		inputs = append(inputs, consumer)
		stoppers = append(stoppers, cancel)
	}

	// Kafka start
	if viper.GetBool("input.kafka.enabled") {
		ctx, cancel := context.WithCancel(context.Background())
		consumer, err := kafka.NewConsumer(&kafka.Config{
			Brokers:       viper.GetStringSlice("input.kafka.host"),
			ConsumerGroup: viper.GetString("input.kafka.group"),
			Ctx:           ctx,
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
			OffsetMode: func() kafka.OffsetMode {
				switch viper.GetString("input.kafka.mode") {
				case "beginning":
					return kafka.OffsetEarliest
				case "latest":
					return kafka.OffsetLatest
				default:
					return kafka.OffsetLastCommit
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
		inputs = append(inputs, consumer)
		stoppers = append(stoppers, cancel)
	}
	return inputs, stoppers
}
