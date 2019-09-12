package run

import (
	"fmt"
	"sync"

	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	"github.com/ccdcoe/go-peek/pkg/models/events"
	log "github.com/sirupsen/logrus"
)

type eventMapFn func(string) events.Atomic

func spawnWorkers(
	rx <-chan *consumer.Message,
	workers int,
	fn eventMapFn,
) <-chan *consumer.Message {
	tx := make(chan *consumer.Message)
	var wg sync.WaitGroup
	go func() {
		defer close(tx)
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
						// TODO - error channel and handle errors externally
						log.Error(err)
						continue loop
					}
					fmt.Println(e.Source())
					tx <- msg
				}
			}(i)
		}
		wg.Wait()
	}()
	return tx
}
