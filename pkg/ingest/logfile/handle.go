package logfile

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	"github.com/ccdcoe/go-peek/pkg/models/events"
	"github.com/ccdcoe/go-peek/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type StatFileIntervalFunc func(first, last []byte) (utils.Interval, error)

type Content int

const (
	Octet Content = iota
	Plaintext
	Gzip
	Xz
	Bzip
	Utf8
	Utf16
)

func (c Content) String() string {
	switch c {
	case Gzip:
		return "application/gzip"
	default:
		return "application/octet-stream"
	}
}

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

func NewHandle(path Path, stat bool, fn StatFileIntervalFunc, atomic events.Atomic) (*Handle, error) {
	if fn == nil {
		return nil, &utils.ErrFuncMissing{
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
		Path:     path,
		Content:  mime,
		Interval: &utils.Interval{},
		Atomic:   atomic,
	}

	h, err := open(s.Path.String())
	if err != nil {
		return s, err
	}
	defer h.Close()

	if !stat {
		return s, nil
	}
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
		case utils.ErrInvalidInterval:
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

func DrainTo(h Handle, ctx context.Context, tx chan<- *consumer.Message, done *sync.WaitGroup) error {
	f, err := open(h.Path.String())
	if err != nil {
		return err
	}

	if h.Offsets == nil {
		h.Offsets = &consumer.Offsets{}
	}

	do := func(
		tx chan<- *consumer.Message,
		f io.ReadCloser,
		from,
		to int64,
		ctx context.Context,
		done *sync.WaitGroup,
	) error {
		defer f.Close()
		defer done.Done()

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
				Event:  h.Atomic,
			}

			if to > 0 && count == to {
				break loop
			}

			count++
		}
		return scanner.Err()
	}

	return do(tx, f, h.Offsets.Beginning, h.Offsets.End, ctx, done)
}

func Drain(h Handle, ctx context.Context) <-chan *consumer.Message {
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

			out := make([]byte, len(scanner.Bytes()))
			copy(scanner.Bytes(), out)

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
