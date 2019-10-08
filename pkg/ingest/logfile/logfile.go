package logfile

import (
	"context"
	"fmt"
	"sync"

	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	"github.com/ccdcoe/go-peek/pkg/models/events"
	"github.com/ccdcoe/go-peek/pkg/utils"

	log "github.com/sirupsen/logrus"
)

type Consumer struct {
	h        []*Handle
	tx       chan *consumer.Message
	conf     Config
	ctx      context.Context
	stoppers utils.WorkerStoppers
	workers  int
}

func NewConsumer(c *Config) (*Consumer, error) {
	if c == nil {
		return nil, fmt.Errorf("logfile consumer is missing config")
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	l := &Consumer{
		h:        make([]*Handle, 0),
		tx:       make(chan *consumer.Message, 0),
		conf:     *c,
		ctx:      c.Ctx,
		stoppers: utils.NewWorkerStoppers(c.ConsumeWorkers),
		workers:  c.ConsumeWorkers,
	}
	for i, dir := range c.Paths {
		log.WithFields(log.Fields{
			"workers": l.conf.StatWorkers,
			"dir":     dir,
		}).Tracef("%d - invoking async stat", i)
		files, err := AsyncStatAll(
			dir,
			l.conf.StatFunc,
			l.conf.StatWorkers,
			false,
			func() events.Atomic {
				if c.MapFunc == nil {
					return events.SimpleE
				}
				return c.MapFunc(dir)
			}(),
		)
		if err != nil {
			return nil, err
		}
		l.h = append(l.h, files...)
	}

	files := make(chan *Handle, 0)
	go func(ctx context.Context) {
		defer l.close()
		defer close(files)
	loop:
		for _, h := range l.h {
			select {
			case <-ctx.Done():
				break loop
			default:
				files <- h
			}
		}
	}(l.ctx)

	var wg sync.WaitGroup
	go func() {
		defer close(l.tx)
		defer func() {
			log.Tracef("logfile consume workers done")
		}()
		for i := 0; i < c.ConsumeWorkers; i++ {
			wg.Add(1)
			go func(id int, ctx context.Context) {
				defer wg.Done()
				defer func() {
					log.WithFields(log.Fields{
						"type": "file", "fn": "reader done", "worker": id,
					}).Trace()
				}()
				log.WithFields(log.Fields{
					"type": "file", "fn": "reader spawn", "worker": id,
				}).Trace()
				for h := range files {
					log.WithFields(log.Fields{
						"type": "file", "fn": "file read", "worker": id,
					}).Trace()
					DrainTo(*h, ctx, l.tx)
				}
			}(i, l.stoppers[i].Ctx)
		}
		wg.Wait()
	}()
	return l, nil
}

// Messages implements consumer.Messager
func (c Consumer) Messages() <-chan *consumer.Message { return c.tx }
func (c Consumer) Files() []string {
	f := make([]string, 0)
	for _, h := range c.h {
		f = append(f, h.Path.String())
	}
	return f
}
func (c Consumer) close() error {
	if c.stoppers == nil || len(c.stoppers) != c.workers {
		return fmt.Errorf("Log file consumer not instanciated properly, cannot close")
	}
	log.Trace("Stopping all log readers")
	for _, s := range c.stoppers {
		s.Cancel()
	}
	log.Tracef("%d log readers stopped", len(c.stoppers))
	return nil
}
