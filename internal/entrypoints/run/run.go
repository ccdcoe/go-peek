package run

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/ccdcoe/go-peek/internal/engines/inputs"
	"github.com/ccdcoe/go-peek/internal/engines/shipper"
	"github.com/ccdcoe/go-peek/pkg/models/consumer"
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

	if err := shipper.Send(modified); err != nil {
		log.Fatal(err)
	}
}
