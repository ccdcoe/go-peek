package replay

import (
	"encoding/json"
	"sync"

	"github.com/ccdcoe/go-peek/internal/ingest/v2/logfile"
	"github.com/ccdcoe/go-peek/pkg/models/events"
	"github.com/ccdcoe/go-peek/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type SequenceList []*Sequence

func (s SequenceList) asyncBuildOrLoadAll(
	spooldir string,
	cache bool,
) error {

	var (
		errs     = make(chan error, s.handleCount())
		handleCh = make(chan *Handle, 0)
		wg       sync.WaitGroup
	)

	go func() {
		wg.Add(1)
		defer wg.Done()
		defer log.Tracef("all %d timestamp collectors exited properly", Workers)

		for i := 0; i < Workers; i++ {
			wg.Add(1)

			go func(i int, rx <-chan *Handle) {
				log.Tracef("Spawning worker %d", i)

				defer wg.Done()
				defer log.Tracef("Worker %d exited", i)

				for h := range rx {

					if cache {

						log.Trace("Cache enabled")
						if err := storeOrLoadCache(h, spooldir); err != nil {
							errs <- err
						}

					} else {

						if err := h.build(); err != nil {
							errs <- err
						}
					}
				}

			}(i, handleCh)
		}

	}()

	for _, seq := range s {
		for _, f := range seq.Files {
			handleCh <- f
		}
	}

	log.Trace("done sending handles to timestamp builders")
	close(handleCh)
	log.Trace("handle channel closed correctly")
	wg.Wait()

	if len(errs) > 0 {
		return utils.ErrChan{
			Desc:  "building from sequence list",
			Items: errs,
		}
	}
	return nil
}

func (s SequenceList) handleCount() int {
	count := 0
	for _, seq := range s {
		count += len(seq.Files)
	}
	return count
}

func (s SequenceList) seekAll(interval utils.Interval) error {
outer:
	for _, seq := range s {
		if seq.Files == nil || len(seq.Files) == 0 {
			continue outer
		}
	inner:
		for _, h := range seq.Files {
			if h.Partial == CompletelyInRange {
				continue inner
			}
			if err := h.seek(interval); err != nil {
				return err
			}
		}
	}
	return nil
}

// TODO: refactor
// TODO: timeParseFunc for arbitrary byte stream instead of always assuming JSON with known keys
// silent fail is best fail
func (s SequenceList) calcDiffBeginning(i utils.Interval) SequenceList {
	if s == nil || len(s) == 0 {
		return s
	}
outer:
	for _, seq := range s {
		if seq.Files == nil || len(seq.Files) == 0 {
			continue outer
		}
	inner:
		for _, h := range seq.Files {
			if h.Enabled {
				line, err := logfile.GetLine(*h.Handle, h.Offsets.Beginning)
				if err == nil {
					var obj events.KnownTimeStamps
					if err := json.Unmarshal(line, &obj); err == nil {
						h.Diffs[h.Offsets.Beginning] = obj.Time().Sub(i.Beginning)
					}
				}
				break inner
			}
		}
	}
	return s
}

func (s SequenceList) calcDiffsBetweenFiles() SequenceList {
	for _, seq := range s {
		seq.calcDiffsBetweenFiles()
	}
	return s
}
