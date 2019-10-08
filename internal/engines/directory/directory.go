package directory

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/ccdcoe/go-peek/pkg/ingest/logfile"
	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	"github.com/ccdcoe/go-peek/pkg/models/events"
	"github.com/ccdcoe/go-peek/pkg/utils"
)

var (
	// IgnoreParseErrors defines weather an individual message parse error should stop processing altogether
	// Alternatively, files will be read to end and total number of encountered errors will be retruned as an error type
	IgnoreParseErrors = false
	// TimeStampFormat is for parsing time range from cli arguments
	TimeStampFormat = "2006-01-02 15:04:05"
)

type Partial int

const (
	CompletelyInRange Partial = iota
	HeadInRange
	TailInRange
	MiddleSectionInRange
)

func (p Partial) string() string {
	switch p {
	case HeadInRange:
		return "handle head is in specified range"
	case TailInRange:
		return "handle tail is in specified range"
	case MiddleSectionInRange:
		return "handle middle section is in specified range"
	default:
		return "handle is fully or not at all in range"
	}
}

type Format int

const (
	JSON Format = iota
	Gob
)

func (f Format) Ext() string {
	switch f {
	case JSON:
		return ".json"
	case Gob:
		return ".gob"
	default:
		return ".dump"
	}
}

// Handle essentially wraps logfile.Handle but also stores time.Duration diffs between each message
type Handle struct {
	*logfile.Handle
	Diffs []time.Duration
	Partial
	Enabled bool
}

func (h *Handle) Enable() *Handle {
	h.Enabled = true
	return h
}

func (h *Handle) CheckPartial(rng utils.Interval) *Handle {
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

func (h *Handle) Seek(interval utils.Interval) error {
	if h.Offsets == nil {
		h.Offsets = &consumer.Offsets{}
	}

	if h.CheckPartial(interval).Partial == CompletelyInRange {
		return nil
	}

	var (
		ctx, cancel = context.WithCancel(context.Background())
		lines       = logfile.Drain(*h.Handle, ctx)
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

func (h *Handle) Build() error {
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

	msgs := logfile.Drain(*h.Handle, context.Background())

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
