package decoder

import (
	"fmt"
	"os"
	"os/signal"
	"sync"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/ccdcoe/go-peek/events"
)

type Decoder struct {
	Input      *cluster.Consumer
	Output     chan interface{}
	Run        bool
	EventTypes map[string]string
	Errors     <-chan error

	errs chan error
	stop []chan bool
}

func NewMessageDecoder(
	workers int,
	input *cluster.Consumer,
	eventtypes map[string]string,
) (*Decoder, error) {
	if eventtypes == nil || len(eventtypes) == 0 {
		return nil, fmt.Errorf("Event Type map undefined")
	}
	var (
		d = &Decoder{
			Run:        true,
			Input:      input,
			Output:     make(chan interface{}),
			EventTypes: eventtypes,
			errs:       make(chan error, 256),
			stop:       make([]chan bool, workers),
		}
		wg sync.WaitGroup
	)

	for i := range d.stop {
		d.stop[i] = make(chan bool, 1)
	}
	d.Errors = d.errs

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	go func() {
		<-signals
		d.halt()
	}()

	go func() {
		defer close(d.Output)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go DecodeWorker(*d, &wg, i)
		}
		wg.Wait()
	}()

	return d, nil
}

func DecodeWorker(d Decoder, wg *sync.WaitGroup, id int) {
	defer wg.Done()
loop:
	for {
		select {
		case msg, ok := <-d.Input.Messages():
			if !ok {
				break loop
			}
			if ev, err := events.NewEvent(
				d.EventTypes[msg.Topic],
				msg.Value,
			); err != nil {
				d.errs <- err
			} else if ev != nil {
				d.Output <- ev
				d.Input.MarkOffset(msg, "")
			}
		case <-d.stop[id]:
			break loop
		}
	}
}

func (d Decoder) halt() {
	for i := range d.stop {
		d.stop[i] <- true
	}
}

func (d Decoder) sendErr(err error) {
	if len(d.errs) == 256 {
		<-d.errs
	}
	d.errs <- err
}
