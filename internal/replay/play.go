package replay

import (
	"context"
	"sync"
	"time"

	"github.com/ccdcoe/go-peek/internal/ingest/v2/logfile"
	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	"github.com/ccdcoe/go-peek/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func play(collection []*Sequence, interval utils.Interval) <-chan *consumer.Message {
	log.Trace("starting the replay")
	tx := make(chan *consumer.Message, 100)

	var wg sync.WaitGroup
	go func() {
		defer close(tx)

		for _, seq := range collection {
			wg.Add(1)
			go func(s Sequence) {
				defer wg.Done()
				var fle *Handle

				for _, fle = range s.Files {

					log.WithFields(log.Fields{
						"file":    fle.Handle.Path.String(),
						"enabled": fle.Enabled,
					}).Trace("iter")

					if !fle.Enabled {
						continue
					}

					log.WithFields(log.Fields{
						"file":  fle.Handle.Path.String(),
						"lines": fle.Lines,
						"from":  fle.Offsets.Beginning,
						"to":    fle.Offsets.End,
						"diffs": len(fle.Diffs),
					}).Debug("reading from input")
					lines := logfile.DrainHandle(*fle.Handle, context.Background())

					log.Trace(fle.Diffs[0])
					diffs := make(chan time.Duration, fle.Offsets.Len())
					for _, di := range fle.Diffs[fle.Offsets.Beginning : fle.Offsets.End-1] {
						diffs <- di
					}

					log.WithFields(log.Fields{
						"diffs": len(diffs),
						"file":  fle.Handle.Path.String(),
					}).Trace("replaying")

					for l := range lines {
						if len(diffs) > 0 {
							time.Sleep(<-diffs)
						}
						tx <- l
					}

					log.WithFields(log.Fields{
						"len":  len(diffs),
						"file": fle.Handle.Path.String(),
					}).Trace("done, stopping replay")

					close(diffs)
				}
			}(*seq)
		}
		wg.Wait()
	}()
	return tx
}
