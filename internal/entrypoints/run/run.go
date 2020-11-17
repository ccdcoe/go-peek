package run

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"go-peek/internal/engines/inputs"
	"go-peek/internal/engines/shipper"
	"go-peek/pkg/ingest/kafka"
	"go-peek/pkg/intel/assets"
	"go-peek/pkg/models/consumer"
	"go-peek/pkg/models/events"
	"go-peek/pkg/utils"

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
	inputs, stoppers := inputs.Create(Workers, spooldir)

	// handle ctrl-c exit
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		go func() {
			timeout := 10 * time.Second
			fallback := time.NewTicker(timeout)
			<-fallback.C
			log.Fatalf(
				"SIGINT handler unable to stop stream, forcing after %d second timeout",
				int(timeout.Seconds()),
			)
		}()
		for _, stop := range stoppers {
			stop()
		}
	}()

	mapping := func() consumer.ParseMap {
		out := make(consumer.ParseMap)
		for _, event := range events.Atomics {
			p := viper.GetString(fmt.Sprintf("stream.%s.parser", event))
			m := consumer.ParseMapping{
				Atomic: event,
				Parser: consumer.NewParser(p),
			}
			if src := viper.GetStringSlice(
				fmt.Sprintf("stream.%s.kafka.topic", event.String()),
			); len(src) > 0 {
				for _, item := range src {
					out[item] = m
				}
			}
			if src := viper.GetStringSlice(
				fmt.Sprintf("stream.%s.dir", event.String()),
			); len(src) > 0 {
				for _, item := range src {
					out[item] = m
				}
			}
			if src := viper.GetStringSlice(
				fmt.Sprintf("stream.%s.uxsock", event.String()),
			); len(src) > 0 {
				for _, item := range src {
					out[item] = m
				}
			}
		}
		return out
	}()

	assetCache := func() *assets.Handle {
		if !viper.GetBool("processor.assets.enabled") {
			return nil
		}
		log.Info("spawning asset cache")
		cache, err := assets.NewHandle(assets.Config{
			Kafka: kafka.Config{
				Brokers:       viper.GetStringSlice("processor.assets.kafka.host"),
				ConsumerGroup: viper.GetString("processor.assets.kafka.group"),
				Topics:        []string{viper.GetString("processor.assets.kafka.topic")},
				Ctx:           context.TODO(),
				OffsetMode:    kafka.TranslateOffsetMode(viper.GetString("processor.assets.kafka.mode")),
			},
		})
		if err != nil {
			log.Fatalf("asset cache spawn: %s", err)
		}
		go func(ctx context.Context) {
			tick := time.NewTicker(30 * time.Second)
		loop:
			for {
				select {
				case <-tick.C:
					missingHosts := cache.GetMissingHosts()
					if len(missingHosts) > 0 {
						for _, host := range missingHosts {
							log.Warnf("Seen following host in logs missing in asset cache -> %s", host)
						}
					}
				case <-ctx.Done():
					break loop
				}
			}
		}(context.TODO())
		return cache
	}()

	channels, errs := spawnWorkers(
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
		mapping,
		assetCache,
	)
	if assetCache != nil {
		assetCache.SetErrs(errs)
	}

	go func(ctx context.Context) {
		if l := log.GetLevel(); l > log.DebugLevel {
			for err := range errs.Items {
				log.Error(err)
			}
		} else {
			tick := time.NewTicker(3 * time.Second)
		loop:
			for {
				select {
				case <-tick.C:
					if errs.Total > 0 {
						log.Error(errs)
					}
				case <-ctx.Done():
					break loop
				}
			}
		}
	}(context.TODO())

	var wg sync.WaitGroup
	wg.Add(1)
	go func(ch <-chan *consumer.Message, wg *sync.WaitGroup) {
		defer wg.Done()
		if err := shipper.Send(ch, "output"); err != nil {
			log.Fatal(err)
		}
		log.Info("Main channel done")
	}(channels.main, &wg)
	if channels.emit != nil {
		wg.Add(1)
		go func(ch <-chan *consumer.Message, wg *sync.WaitGroup) {
			defer wg.Done()
			if err := shipper.Send(ch, "output"); err != nil {
				log.Fatal(err)
			}
			log.Info("Emitter done")
		}(channels.emit, &wg)
	}
	wg.Wait()
	log.Info("All done!")
}
