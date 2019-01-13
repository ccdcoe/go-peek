package decoder

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/ccdcoe/go-peek/internal/logging"
	"github.com/ccdcoe/go-peek/internal/types"
	"github.com/ccdcoe/go-peek/pkg/events"
)

const defaultWorkerCount = 4

type Decoder struct {
	input  types.Messager
	output chan types.Message

	EventTypes map[string]string

	workerStoppers   []context.CancelFunc
	inventoryStopper context.CancelFunc

	inventory       *types.ElaTargetInventory
	inventoryConfig *types.ElaTargetInventoryConfig

	rename *Rename

	workers   int
	logsender logging.LogHandler
}

func NewMessageDecoder(
	config DecoderConfig,
) (*Decoder, error) {
	var (
		d   = &Decoder{}
		wg  sync.WaitGroup
		err error
	)

	if config.EventMap == nil || len(config.EventMap) == 0 {
		return nil, fmt.Errorf("Event Type map undefined")
	}
	d.EventTypes = config.EventMap

	if config.Input == nil {
		return nil, fmt.Errorf("Decoder input missing")
	}
	d.input = config.Input
	d.output = make(chan types.Message, 0)

	if config.Workers > 0 {
		d.workers = config.Workers
	} else {
		d.workers = defaultWorkerCount
	}

	if config.LogHandler == nil {
		d.logsender = logging.NewLogHandler()
	} else {
		d.logsender = config.LogHandler
	}

	if d.rename, err = NewRename(
		RenameConfig{SpoolDir: config.Spooldir},
	); err != nil {
		return nil, err
	}

	if !config.IgnoreSigInt {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)
		go func() {
			<-signals
			d.halt()
		}()
	}

	if config.InventoryConfig != nil {
		d.inventoryConfig = config.InventoryConfig
	}
	if err = d.UpdateInventoryAndMaps(); err != nil {
		return nil, err
	}

	d.workerStoppers = make([]context.CancelFunc, config.Workers)
	go func() {
		var ctx context.Context
		defer close(d.output)
		for i := 0; i < config.Workers; i++ {
			wg.Add(1)
			ctx, d.workerStoppers[i] = context.WithCancel(context.Background())
			go DecodeWorker(*d, &wg, ctx)
		}
		wg.Wait()

		if err = d.rename.SaveMappings(); err != nil {
			d.logsender.Error(err)
		}
		if err = d.rename.SaveNames(); err != nil {
			d.logsender.Error(err)
		}
	}()

	var ctx context.Context
	ctx, d.inventoryStopper = context.WithCancel(context.Background())
	go func(ctx context.Context) {
		var (
			err  error
			poll = time.NewTicker(30 * time.Second)
		)
	loop:
		for {
			select {
			case <-poll.C:
				if err = d.UpdateInventoryAndMaps(); err != nil {
					d.logsender.Error(err)
				}
			case <-ctx.Done():
				break loop
			}
		}
	}(ctx)

	return d, nil
}

func (d Decoder) Logs() logging.LogListener {
	return d.logsender
}

func (d Decoder) Messages() <-chan types.Message {
	return d.output
}

func (d *Decoder) UpdateInventoryAndMaps() error {
	if d.inventoryConfig == nil {
		d.logsender.Notify("Inventory config missing, not loading grains")
		return nil
	}
	d.inventory = types.NewElaTargetInventory()
	if err := d.inventory.Get(*d.inventoryConfig); err != nil {
		return err
	}
	// *TODO* I'm sure something can fail here
	d.rename.IpToStringName = types.NewStringValues(
		d.inventory.MapKnownIP(
			d.rename.ByName.RawValues(),
		),
	)
	return nil
}

func (d Decoder) getAssetIpMap() events.AssetIpMap {
	if d.rename != nil && d.rename.IpToStringName != nil {
		return events.AssetIpMap(d.rename.IpToStringName.RawValues())
	}
	return events.AssetIpMap(map[string]string{})
}

func (d Decoder) halt() {
	for i := range d.workerStoppers {
		d.workerStoppers[i]()
	}
	d.inventoryStopper()
}

func (d Decoder) Names() map[string]string {
	return d.rename.ByName.RawValues()
}

func (d Decoder) IPmaps() map[string]string {
	return d.rename.IpToStringName.RawValues()
}

func DecodeWorker(d Decoder, wg *sync.WaitGroup, ctx context.Context) {
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

		formats = make(map[string]string)
	)
	defer updateAssetMap.Stop()
loop:
	for {
		select {
		case msg, ok := <-d.input.Messages():
			if !ok {
				break loop
			}
			eventType := msg.Source
			if val, ok := d.EventTypes[msg.Source]; ok {
				eventType = val
			}
			if ev, err = events.NewEvent(
				eventType,
				msg.Data,
			); err != nil {
				d.logsender.Error(err)
				continue loop
			}

			if shipper, err = ev.Source(); err != nil {
				d.logsender.Error(err)
				continue loop
			}

			if pretty, seen = d.rename.Check(shipper.IP.String()); !seen {
				d.logsender.Notify(fmt.Sprintf(
					"New host %s, ip %s observed, name will be %s",
					shipper.Host,
					shipper.IP,
					pretty,
				))
			}
			if d.rename != nil {
				ev.Rename(pretty)
				ip2name.CheckSetSource(shipper)
			}

			if data, err = ev.JSON(); err != nil {
				d.logsender.Error(err)
				continue loop
			}
			if sagan, err = ev.SaganString(); err != nil {
				switch err.(type) {
				case *types.ErrNotImplemented:
					sagan = ""
				default:
					d.logsender.Error(err)
				}
			}
			formats["sagan"] = sagan
			d.output <- types.Message{
				Data:    data,
				Source:  msg.Source,
				Offset:  msg.Offset,
				Key:     ev.Key(),
				Time:    ev.GetEventTime(),
				Formats: formats,
			}
		case <-ctx.Done():
			break loop
		case <-updateAssetMap.C:
			ip2name = d.getAssetIpMap()
		}
	}
}
