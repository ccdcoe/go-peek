package logfile

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	"github.com/ccdcoe/go-peek/pkg/models/events"
	"github.com/ccdcoe/go-peek/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// Handle is a container for commonly needed information about log file
// Path, number of lines, beginning and end times, etc
// Keep it separate from logfile.Path, as parsing timestamps and counting lines can take significant amount of time
type Handle struct {
	Path
	Lines int64
	Content

	Interval *utils.Interval
	Offsets  *consumer.Offsets
	Atomic   events.Atomic
}

func newHandle(path Path, fn utils.StatFileIntervalFunc) (*Handle, error) {
	if fn == nil {
		return nil, &ErrFuncMissing{
			Caller: fmt.Sprintf("Handle %s", path.String()),
			Func:   "File interval parse",
		}
	}

	var (
		mime Content
		err  error
	)

	if mime, err = GetFileContentType(path.String()); err != nil {
		return nil, err
	}

	s := &Handle{
		Path:    path,
		Content: mime,
	}

	h, err := open(s.Path.String())
	if err != nil {
		return s, err
	}
	defer h.Close()

	first, last, lines, err := statLogFileSinglePass(h)

	s.Lines = lines
	if err != nil {
		return s, err
	}

	s.Offsets = &consumer.Offsets{
		Beginning: 0,
		End:       lines,
	}

	interval, err := fn(first, last)
	if err != nil {
		return s, err
	}
	s.Interval = &interval

	if err := s.Interval.Validate(); err != nil {
		switch e := err.(type) {
		case *utils.ErrInvalidInterval:
			return s, e.SetSrc(path.String())
		}
		return s, err
	}

	return s, nil
}

func GetLine(h Handle, num int64) ([]byte, error) {
	f, err := open(h.Path.String())
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	var count int64
	for scanner.Scan() {
		if count == num {
			return utils.DeepCopyBytes(scanner.Bytes()), nil
		}
		count++
	}
	return nil, scanner.Err()
}

func DrainHandle(h Handle, ctx context.Context) <-chan *consumer.Message {
	f, err := open(h.Path.String())
	if err != nil {
		return nil
	}

	if h.Offsets == nil {
		h.Offsets = &consumer.Offsets{}
	}

	tx := make(chan *consumer.Message, 0)

	go func(tx chan *consumer.Message, f io.ReadCloser, from, to int64, ctx context.Context) {

		defer close(tx)
		defer f.Close()

		scanner := bufio.NewScanner(f)
		var count int64

	loop:
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				log.Tracef("Scanner break received for %s, exiting", h.Path.String())
				break loop
			default:
			}

			if count < from {
				count++
				continue loop
			}

			tx <- &consumer.Message{
				Data:   utils.DeepCopyBytes(scanner.Bytes()),
				Offset: count,
				Type:   consumer.Logfile,
				Source: h.Path.String(),
				Key:    h.Atomic.String(),
			}

			if to > 0 && count == to {
				break loop
			}

			count++
		}

	}(tx, f, h.Offsets.Beginning, h.Offsets.End, ctx)

	return tx
}
