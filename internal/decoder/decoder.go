package decoder

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/ccdcoe/go-peek/internal/types"
	"github.com/ccdcoe/go-peek/pkg/events"
)

const (
	errBufSize    = 256
	notifyBufSize = 256
)

type DecodedMessage struct {
	Val  []byte
	Time time.Time

	Topic, Key, Sagan string
}

type Decoder struct {
	Input         *cluster.Consumer
	Output        chan DecodedMessage
	Run           bool
	EventTypes    map[string]string
	Errors        <-chan error
	Notifications <-chan string

	errs          chan error
	notify        chan string
	stop          []chan bool
	stopInventory chan bool

	rename *Rename
}

func NewMessageDecoder(
	workers int,
	input *cluster.Consumer,
	eventtypes map[string]string,
	spooldir string,
	inventoryHost string,
	inventoryIndex string,
) (*Decoder, error) {
	if eventtypes == nil || len(eventtypes) == 0 {
		return nil, fmt.Errorf("Event Type map undefined")
	}
	var (
		d = &Decoder{
			Run:           true,
			Input:         input,
			Output:        make(chan DecodedMessage),
			EventTypes:    eventtypes,
			errs:          make(chan error, errBufSize),
			notify:        make(chan string, notifyBufSize),
			stop:          make([]chan bool, workers),
			stopInventory: make(chan bool, 1),
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

	// MOVE THIS PART
	inventory := &types.ElaTargetInventory{}
	if err := inventory.Get(inventoryHost, inventoryIndex); err != nil {
		return nil, err
	}
	d.rename.IpToStringName = types.NewStringValues(
		inventory.MapKnownIP(
			d.rename.ByName.RawValues(),
		),
	)
	// END MOVE THIS PART

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

	go func() {
		poll := time.NewTicker(30 * time.Second)
	loop:
		for {
			select {
			case <-poll.C:
				d.sendNotify("refreshing inventory")
				inventory := &types.ElaTargetInventory{}
				if err := inventory.Get(inventoryHost, inventoryIndex); err != nil {
					d.sendErr(err)
					continue loop
				}
				d.rename.IpToStringName = types.NewStringValues(
					inventory.MapKnownIP(
						d.rename.ByName.RawValues(),
					),
				)
			case <-d.stopInventory:
				d.sendNotify("stopping inventory refresh routine")
				break loop
			}
		}
	}()

	return d, nil
}

func DecodeWorker(d Decoder, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	// Local variables
	var (
		err error

		ev      events.Event
		shipper *events.Source

		data []byte

		pretty string
		sagan  string
		seen   bool

		ip2name        = d.getAssetIpMap()
		updateAssetMap = time.NewTicker(5 * time.Second)
	)
	defer updateAssetMap.Stop()
loop:
	for {
		select {
		case msg, ok := <-d.Input.Messages():
			if !ok {
				break loop
			}
			if ev, err = events.NewEvent(
				d.EventTypes[msg.Topic],
				msg.Value,
			); err != nil {
				d.sendErr(err)
				continue loop
			}

			if shipper, err = ev.Source(); err != nil {
				d.sendErr(err)
				continue loop
			}

			if pretty, seen = d.rename.Check(shipper.IP.String()); !seen {
				d.sendNotify(fmt.Sprintf(
					"New host %s, ip %s observed, name will be %s",
					shipper.Host,
					shipper.IP,
					pretty,
				))
			}
			ev.Rename(pretty)
			ip2name.CheckSetSource(shipper)

			if data, err = ev.JSON(); err != nil {
				d.sendErr(err)
				continue loop
			}
			if sagan, err = ev.SaganString(); err != nil {
				switch err.(type) {
				case *types.ErrNotImplemented:
					sagan = ""
				default:
					d.sendErr(err)
				}
			}
			d.Output <- DecodedMessage{
				Val:   data,
				Topic: msg.Topic,
				Key:   ev.Key(),
				Time:  ev.GetEventTime(),
				Sagan: sagan,
			}
			d.Input.MarkOffset(msg, "")
		case <-d.stop[id]:
			break loop
		case <-updateAssetMap.C:
			ip2name = d.getAssetIpMap()
		}
	}
}

func (d Decoder) getAssetIpMap() events.AssetIpMap {
	return events.AssetIpMap(d.rename.IpToStringName.RawValues())
}

func (d Decoder) halt() {
	for i := range d.stop {
		d.stop[i] <- true
	}
	d.stopInventory <- true
}

func (d Decoder) sendErr(err error) {
	if len(d.errs) == errBufSize {
		<-d.errs
	}
	d.errs <- err
}

func (d Decoder) sendNotify(msg string) {
	if len(d.notify) == notifyBufSize {
		<-d.notify
	}
	d.notify <- fmt.Sprintf("[decoder] %s", msg)
}

func (d Decoder) Names() map[string]string {
	return d.rename.ByName.RawValues()
}

func (d Decoder) IPmaps() map[string]string {
	return d.rename.IpToStringName.RawValues()
}
