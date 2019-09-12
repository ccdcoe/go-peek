package replay

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/ccdcoe/go-peek/internal/ingest/logfile"
	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	events "github.com/ccdcoe/go-peek/pkg/models/events"
	"github.com/ccdcoe/go-peek/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// Handle essentially wraps logfile.Handle but also stores time.Duration diffs between each message
type Handle struct {
	*logfile.Handle
	Diffs []time.Duration
	Partial
	Enabled bool
}

func (h *Handle) enable() *Handle {
	h.Enabled = true
	return h
}

func (h *Handle) checkPartial(rng utils.Interval) *Handle {
	if utils.IntervalContains(*h.Interval, rng) {
		h.Partial = MiddleSectionInRange
	} else if utils.IntervalHeadInRange(*h.Interval, rng) {
		h.Partial = HeadInRange
	} else if utils.IntervalTailInRange(*h.Interval, rng) {
		h.Partial = TailInRange
	}
	return h
}

func (h Handle) ID() string {
	return fmt.Sprintf(
		"timestamp-diffs-handle-%x",
		sha256.Sum256([]byte(h.Handle.Path.String())),
	)
}

func (h Handle) len() int {
	return len(h.Diffs)
}

func (h *Handle) seek(interval utils.Interval) error {
	if h.Offsets == nil {
		h.Offsets = &consumer.Offsets{}
	}

	if h.checkPartial(interval).Partial == CompletelyInRange {
		return nil
	}

	var (
		ctx, cancel = context.WithCancel(context.Background())
		lines       = logfile.DrainHandle(*h.Handle, ctx)
		obj         events.KnownTimeStamps
		count       int64
		first, last bool
	)

	log.Tracef("%s %s", h.Path.String(), h.Partial.string())
	for l := range lines {
		if err := json.Unmarshal(l.Data, &obj); err != nil {
			cancel()
			return err
		}

		if h.Partial == TailInRange && obj.Time().After(interval.Beginning) {
			if !first {
				h.Offsets.Beginning = count
				cancel()
				first = true
			}
		}

		if h.Partial == HeadInRange && obj.Time().After(interval.End) {
			if !last {
				h.Offsets.End = count
				cancel()
				last = true
			}
			cancel()
		}

		if h.Partial == MiddleSectionInRange {
			if !first && obj.Time().After(interval.Beginning) {
				h.Offsets.Beginning = count
				first = true
			}
			if !last && obj.Time().After(interval.End) {
				h.Offsets.End = count
				last = true
				cancel()
			}
		}

		count++
	}
	cancel()
	return nil
}

func (h *Handle) build() error {
	if h == nil {
		return &utils.ErrNilPointer{
			Caller:   "handle build",
			Function: "self",
		}
	}

	dfs := make([]time.Duration, h.Lines)

	start := time.Now()
	defer log.WithFields(log.Fields{
		"path":         h.Path,
		"diffs":        h.len(),
		"took":         time.Since(start).String(),
		"lines":        h.Lines,
		"offset_start": h.Handle.Offsets.Beginning,
		"offset_end":   h.Handle.Offsets.End,
		"time_start":   h.Handle.Interval.Beginning.Format(TimeStampFormat),
		"time_end":     h.Handle.Interval.End.Format(TimeStampFormat),
	}).Debug("done building diffs")

	var (
		last              time.Time
		obj               events.KnownTimeStamps
		count, errs, fixd int64
		// TODO: code may deadlock if h.Lines is not initialized properly
	)

	msgs := logfile.DrainHandle(*h.Handle, context.Background())

	log.WithFields(log.Fields{
		"file":   h.Handle.Base(),
		"dir":    h.Handle.Dir(),
		"action": "timestamp parse",
	}).Trace("build replay time diffs")

	/*
		if h.Partial == CompletelyInRange {
			h.First = 0
		}
	*/

loop:
	for msg := range msgs {

		if err := json.Unmarshal(msg.Data, &obj); err != nil {
			errs++
			data := events.TryFixBrokenMessage(msg.Data)

			if err := json.Unmarshal(data, &obj); err != nil {
				if !IgnoreParseErrors {

					return &utils.ErrParseRawData{
						Err:    err,
						Raw:    data,
						Source: h.Path.String(),
						Offset: int64(count),
						Desc:   fmt.Sprintf("parsing known timestamps for replay diff gen"),
					}
				}

				continue loop
			}
			fixd++
		}

		/*
			if h.Partial == TailInRange || h.Partial == MiddleSectionInRange {
				if obj.Time().Before(rng.Beginning) {
					h.Handle.Offsets.Beginning++
				}
			}
			if h.Partial == HeadInRange || h.Partial == MiddleSectionInRange {
				if obj.Time().Before(rng.End) {
					h.Handle.Offsets.End = count
				}
			}
		*/

		dfs[count] = obj.Time().Sub(last)
		msg.Time = obj.Time()

		last = obj.Time()
		count++
	}

	log.WithFields(log.Fields{
		"file":   h.Handle.Base(),
		"dir":    h.Handle.Dir(),
		"action": "draining diff channel",
	}).Trace("build replay time diffs")

	// diff is next - last, so first diff would default to first - 1970
	dfs[0] = 0
	h.Diffs = dfs

	if errs > 0 {
		return utils.ErrParseMessageSource{
			Count:  errs,
			Source: h.Path.String(),
			Parser: events.KnownTimeStampsE.String(),
		}
	}

	return nil
}

func newHandleSlice(dir string, atomic events.Atomic) ([]*Handle, error) {
	if Workers < 1 {
		Workers = 1
	}

	log.WithFields(log.Fields{
		"workers": Workers,
		"dir":     dir,
		"action":  "invoking async stat",
	}).Trace("replay sequence discovery")

	files, err := logfile.AsyncStatAll(dir, getIntervalFromJSON, Workers)
	if err != nil {
		return nil, err
	}

	log.WithFields(log.Fields{
		"found":  len(files),
		"dir":    dir,
		"action": "sorting",
	}).Trace("discovery done")

	sort.Slice(files, func(i, j int) bool {
		return files[i].Interval.Beginning.Before(files[j].Interval.Beginning)
	})

	handles := make([]*Handle, len(files))
	for i, h := range files {
		if h == nil {
			return handles, &utils.ErrNilPointer{
				Caller:   "log file discovery",
				Function: fmt.Sprintf("log file no %d while building Handle list", i),
			}
		}
		h.Atomic = atomic
		handles[i] = &Handle{Handle: h}
	}
	return handles, nil
}
