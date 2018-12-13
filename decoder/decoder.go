package decoder

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/ccdcoe/go-peek/events"
	"github.com/ccdcoe/go-peek/types"
)

const defaultName = "Kerrigan"

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
			errs:          make(chan error, 256),
			notify:        make(chan string, 256),
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
				inventory := &types.ElaTargetInventory{}
				d.sendNotify("refreshing inventory")
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

	// Local copy, TODO: thread safe periodic update
	var ip2name = d.rename.IpToStringName.RawValues()
	//fmt.Println(ip2name)
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

				shipper := ev.Source()
				pretty, notseen := d.rename.Check(shipper.IP)

				if notseen {
					d.sendNotify(fmt.Sprintf("New host %s, ip %s observed, name will be %s",
						shipper.Host, shipper.IP, pretty))
				}
				ev.Rename(pretty)

				// TODO! Functions are good
				var (
					srcPretty  = defaultName
					destPretty = defaultName
				)
				if src, ok := shipper.GetSrcIp(); ok {
					if val, ok := ip2name[src]; ok {
						srcPretty = val
					}
				}
				if dest, ok := shipper.GetDestIp(); ok {
					if val, ok := ip2name[dest]; ok {
						destPretty = val
					}
				}

				shipper.SetSrcDestNames(srcPretty, destPretty)
				if json, err := ev.JSON(); err != nil {
					d.sendErr(err)
				} else {
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
	d.stopInventory <- true
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

func (d Decoder) Names() map[string]string {
	return d.rename.ByName.RawValues()
}

func (d Decoder) IPmaps() map[string]string {
	return d.rename.IpToStringName.RawValues()
}
