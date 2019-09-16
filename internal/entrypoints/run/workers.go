package run

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	"github.com/ccdcoe/go-peek/pkg/models/events"
	"github.com/ccdcoe/go-peek/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type eventMapFn func(string) events.Atomic

func spawnWorkers(
	rx <-chan *consumer.Message,
	workers int,
	fn eventMapFn,
) (<-chan *consumer.Message, *utils.ErrChan) {
	tx := make(chan *consumer.Message, 0)
	errs := &utils.ErrChan{
		Desc:  "Event parse worker runtime errors",
		Items: make(chan error, 100),
	}
	var wg sync.WaitGroup
	go func() {
		defer close(tx)
		defer close(errs.Items)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				defer log.Tracef("worker %d done", id)

				log.Tracef("Spawning worker %d", id)

			loop:
				for msg := range rx {
					e, err := events.NewGameEvent(msg.Data, fn(msg.Source))
					if err != nil {
						errs.Send(err)
						continue loop
					}
					fmt.Fprintf(
						os.Stdout,
						"%s: %s: %+v\n",
						fn(msg.Source),
						e.Time().Format(time.Stamp),
						e.GetAsset(),
					)
					tx <- msg
				}
			}(i)
		}
		wg.Wait()
	}()
	return tx, errs
}
