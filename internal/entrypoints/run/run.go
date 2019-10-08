package run

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"

	"github.com/ccdcoe/go-peek/internal/engines/shipper"
	"github.com/ccdcoe/go-peek/internal/helpers"
	inKafka "github.com/ccdcoe/go-peek/pkg/ingest/kafka"
	"github.com/ccdcoe/go-peek/pkg/ingest/logfile"
	"github.com/ccdcoe/go-peek/pkg/ingest/uxsock"
	"github.com/ccdcoe/go-peek/pkg/models/consumer"
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
		consumer, err := inKafka.NewConsumer(&inKafka.Config{
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
		inputs = append(inputs, consumer)
		stoppers = append(stoppers, cancel)
	}
	// handle ctrl-c exit
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		for _, stop := range stoppers {
			stop()
		}
	}()
	modified, errs := spawnWorkers(
		func() <-chan *consumer.Message {
			if len(inputs) == 1 {
				return inputs[0].Messages()
			}
			tx := make(chan *consumer.Message, 0)
			var wg sync.WaitGroup
			go func() {
				defer close(tx)
				for _, iface := range inputs {
					wg.Add(1)
					go func(rx <-chan *consumer.Message, tx chan<- *consumer.Message) {
						defer wg.Done()
						for msg := range rx {
							tx <- msg
						}
					}(iface.Messages(), tx)
				}
				wg.Wait()
			}()
			return tx
		}(),
		Workers,
		spooldir,
	)

	go func() {
		for err := range errs.Items {
			log.Error(err)
		}
	}()

	if err := shipper.Send(modified); err != nil {
		log.Fatal(err)
	}
}
