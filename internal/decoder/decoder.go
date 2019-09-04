package decoder

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"

	"github.com/ccdcoe/go-peek/internal/logging"
	"github.com/ccdcoe/go-peek/internal/types"
	events "github.com/ccdcoe/go-peek/pkg/events/v1"
)

const defaultWorkerCount = 4
const defaultName = "Kerrigan"

type ErrMessedUpRename struct {
	msg  events.EventFormatter
	src  events.Meta
	dest events.Meta
}

func (e ErrMessedUpRename) Error() string {
	data, err := e.msg.JSON()
	msg := ""
	if err != nil {
		msg = string(data)
	}
	return fmt.Sprintf(
		"Src: %s | %s  Dest %s | %s [%s]",
		e.src.IP.String(),
		e.src.Host,
		e.dest.IP.String(),
		e.dest.Host,
		msg,
	)
}

type ErrParseWrong struct {
	offset int64
	source string
	msg    []byte
	parsed []byte
}

func (e ErrParseWrong) Error() string {
	return fmt.Sprintf("Shipper IP missing for msg %d from %s.\n Original is [%s]\n Parsed is [%s]\n",
		e.offset, e.source, string(e.msg), string(e.parsed))
}

type Decoder struct {
	input  types.Messager
	output chan types.Message

	EventTypes map[string]string

	workerStoppers []context.CancelFunc

	NameMappings

	workers   int
	logsender logging.LogHandler

	*sync.Mutex
}

func NewMessageDecoder(
	config DecoderConfig,
) (*Decoder, error) {
	var (
		d  = &Decoder{Mutex: &sync.Mutex{}}
		wg sync.WaitGroup
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

	if n, err := newSyncNamePool(config.Spooldir, *config.InventoryConfig); err != nil {
		return nil, err
	} else {
		d.NameMappings = *n
	}

	if !config.IgnoreSigInt {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)
		go func() {
			<-signals
			d.halt()
		}()
	}

	d.workerStoppers = make([]context.CancelFunc, config.Workers)
	go func() {
		var ctx context.Context
		defer close(d.output)
		for i := 0; i < config.Workers; i++ {
			wg.Add(1)
			d.Lock()
			ctx, d.workerStoppers[i] = context.WithCancel(context.Background())
			d.Unlock()
			go DecodeWorker(d, &wg, ctx)
		}
		wg.Wait()
	}()

	return d, nil
}

func (d Decoder) Logs() logging.LogListener {
	return d.logsender
}

func (d Decoder) Messages() <-chan types.Message {
	return d.output
}

func (d *Decoder) halt() {
	d.Lock()
	for i := range d.workerStoppers {
		d.workerStoppers[i]()
	}
	d.Unlock()
}

func DecodeWorker(d *Decoder, wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()
	// Local variables
	var (
		err error

		ev      events.Event
		shipper *events.Source

		data []byte

		sagan string
	)
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
			if shipper.IP == nil {
				parsed, _ := ev.JSON()
				d.logsender.Error(&ErrParseWrong{
					offset: msg.Offset,
					source: msg.Source,
					msg:    msg.Data,
					parsed: parsed,
				})
				continue loop
			}

			pretty, loaded := d.NameReMap.Load(shipper.IP.String())
			if !loaded {
				pretty = defaultName
			}
			asserted, ok := pretty.(string)
			if !ok {
				d.logsender.Error(fmt.Errorf("inventory rename map type assert fail for %s", shipper.IP))
				continue loop
			}

			ev.Rename(asserted)

			if ip, ok := shipper.GetSrcIp(); ok {
				if val, ok := d.NameReMap.Load(ip.String()); ok {
					v, ok := val.(string)
					if ok {
						shipper.SetSrcName(v)
					}
				} else {
					shipper.SetSrcName(defaultName)
				}
			}

			if ip, ok := shipper.GetDestIp(); ok {
				if val, ok := d.NameReMap.Load(ip.String()); ok {
					v, ok := val.(string)
					if ok {
						shipper.SetDestName(v)
					}
				} else {
					shipper.SetDestName(defaultName)
				}
			}

			if (shipper.Src != nil && shipper.Dest != nil) && (!shipper.Src.IP.Equal(shipper.Dest.IP)) && (shipper.Src.Host == shipper.Dest.Host) && shipper.Src.Host != "Kerrigan" {
				d.logsender.Error(&ErrMessedUpRename{
					dest: *shipper.Dest,
					src:  *shipper.Src,
					msg:  ev,
				})
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
			d.output <- types.Message{
				Data:    data,
				Source:  msg.Source,
				Offset:  msg.Offset,
				Key:     ev.Key(),
				Time:    ev.GetEventTime(),
				Formats: types.Formats{Sagan: sagan},
			}
		case <-ctx.Done():
			break loop
		}
	}
}
