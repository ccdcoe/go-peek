package decoder

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/ccdcoe/go-peek/events"
)

type DecodedMessage struct {
	Val   []byte
	Topic string
	Key   string
	Sagan string
	Time  time.Time
}

type Decoder struct {
	Input         *cluster.Consumer
	Output        chan DecodedMessage
	Run           bool
	EventTypes    map[string]string
	Errors        <-chan error
	Notifications <-chan string

	errs   chan error
	notify chan string
	stop   []chan bool

	rename *Rename
}

func NewMessageDecoder(
	workers int,
	input *cluster.Consumer,
	eventtypes map[string]string,
	spooldir string,
) (*Decoder, error) {
	if eventtypes == nil || len(eventtypes) == 0 {
		return nil, fmt.Errorf("Event Type map undefined")
	}
	var (
		d = &Decoder{
			Run:        true,
			Input:      input,
			Output:     make(chan DecodedMessage),
			EventTypes: eventtypes,
			errs:       make(chan error, 256),
			notify:     make(chan string, 256),
			stop:       make([]chan bool, workers),
		}
		wg  sync.WaitGroup
		err error
	)

	if d.rename, err = NewRename(spooldir); err != nil {
		return nil, err
	}

	for i := range d.stop {
		d.stop[i] = make(chan bool, 1)
	}
	d.Errors = d.errs
	d.Notifications = d.notify

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	go func() {
		<-signals
		d.halt()
	}()

	go func() {
		defer close(d.Output)
		defer close(d.errs)
		defer close(d.notify)

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go DecodeWorker(*d, &wg, i)
		}
		wg.Wait()

		d.sendNotify(fmt.Sprintf("dumping mappings"))
		if err = d.rename.SaveMappings(); err != nil {
			d.errs <- err
		}
		d.sendNotify(fmt.Sprintf("dumping mapped names"))
		if err = d.rename.SaveNames(); err != nil {
			d.errs <- err
		}
		d.sendNotify(fmt.Sprintf("done, exiting"))

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
				d.sendErr(err)
			} else if ev != nil {
				ev.Rename(d.rename.Check(ev.Source().Host))
				if json, err := ev.JSON(); err != nil {
					d.sendErr(err)
				} else {
					//fmt.Println(ev.GetEventTime(), ev.Key())
					d.Output <- DecodedMessage{
						Val:   json,
						Topic: msg.Topic,
						Key:   ev.Key(),
						Time:  ev.GetEventTime(),
						Sagan: ev.SaganString(),
					}
				}
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

func (d Decoder) sendNotify(msg string) {
	if len(d.notify) == 256 {
		<-d.notify
	}
	d.notify <- fmt.Sprintf("[decoder] %s", msg)
}
