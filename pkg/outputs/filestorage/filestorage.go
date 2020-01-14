package filestorage

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	"github.com/ccdcoe/go-peek/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var (
	TimeFmt = "20060102150405"
)

type Config struct {
	Dir       string
	Combined  string
	Gzip      bool
	Timestamp bool

	Stream <-chan consumer.Message

	RotateEnabled  bool
	RotateGzip     bool
	RotateInterval time.Duration
}

func (c *Config) Validate() error {
	if c == nil {
		c = &Config{}
	}
	if c.Stream == nil {
		return fmt.Errorf("missing input stream for filestorage")
	}
	if c.Combined == "" && c.Dir == "" {
		return fmt.Errorf(
			"filestorage module requires either a root directory or explicit destination file for storing events",
		)
	}
	if c.Dir != "" {
		if !utils.StringIsValidDir(c.Dir) {
			return fmt.Errorf("path %s is not valid directory", c.Dir)
		}
	}
	return nil
}

type logFile struct {
	rx   chan consumer.Message
	path string
	done chan bool
}

type Handle struct {
	errs *utils.ErrChan
	rx   <-chan consumer.Message

	filterChannels map[string]*logFile
	filterEnabled  bool

	combinedEnabled bool

	combined string
	dir      string

	timestamp bool
	gzip      bool

	rotateTicker *time.Ticker
	rotate       bool

	mu *sync.Mutex
	wg *sync.WaitGroup
}

func NewHandle(c *Config) (*Handle, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &Handle{
		errs:      utils.NewErrChan(100, "filestorage handle"),
		rx:        c.Stream,
		timestamp: c.Timestamp,
		gzip:      c.Gzip,
		combined:  c.Combined,
		dir:       c.Dir,
		wg:        &sync.WaitGroup{},
		mu:        &sync.Mutex{},
		combinedEnabled: func() bool {
			if c.Combined != "" {
				return true
			}
			return false
		}(),
		filterEnabled: func() bool {
			if c.Dir != "" {
				return true
			}
			return false
		}(),
		rotateTicker:   time.NewTicker(c.RotateInterval),
		rotate:         c.RotateEnabled,
		filterChannels: make(map[string]*logFile),
	}, nil
}

func (h *Handle) Do(ctx context.Context) error {
	// function for formatting output file name
	filenameFunc := func(ts time.Time, path string) string {
		if h.timestamp || h.rotate {
			path = fmt.Sprintf("%s.%s", path, ts.Format(TimeFmt))
		}
		if h.gzip {
			path = fmt.Sprintf("%s.gz", path)
		}
		return path
	}

	h.wg.Add(1)
	combineCh := make(chan consumer.Message, 0)
	if h.filterChannels == nil {
		h.filterChannels = make(map[string]*logFile)
	}
	go func() {
		defer func() {
			if h.filterChannels != nil {
				for _, obj := range h.filterChannels {
					close(obj.rx)
					<-obj.done
				}
			}
		}()
		defer close(combineCh)
		defer h.wg.Done()

	loop:
		for {
			select {
			case msg, ok := <-h.rx:
				if !ok {
					break loop
				}
				if h.combinedEnabled {
					combineCh <- msg
				}

				if h.filterEnabled {
					key := msg.Event.String()
					if obj, ok := h.filterChannels[key]; ok {
						obj.rx <- msg
					} else {
						now := time.Now()
						path := func() string {
							path := fmt.Sprintf("%s", path.Join(h.dir, msg.Event.String()))
							if h.timestamp || h.rotate {
								path = fmt.Sprintf("%s.%s", path, now.Format(TimeFmt))
							}
							return path
						}()
						h.mu.Lock()
						ch := make(chan consumer.Message, 0)
						obj := &logFile{
							path: path,
							rx:   ch,
							done: make(chan bool),
						}
						h.filterChannels[key] = obj
						h.mu.Unlock()

						log.Tracef("creating new log file %s", path)
						if err := writeSingleFile(
							*obj,
							*h.errs,
							false,
							context.TODO(),
							h.wg,
						); err != nil {
							h.errs.Send(err)
						}
					}
				}

			case <-ctx.Done():
				break loop
			case <-h.rotateTicker.C:
				h.mu.Lock()
				for k, v := range h.filterChannels {
					old := v
					close(v.rx)
					delete(h.filterChannels, k)
					log.Tracef("rotated event %s", k)

					go func(source string, done chan bool) {
						h.wg.Add(1)
						defer h.wg.Done()
						<-done
						log.Tracef("compressing %s", old.path)
						if err := utils.GzipCompress(source, fmt.Sprintf("%s.gz", source)); err != nil {
							h.errs.Send(err)
						}
						log.Tracef("done compressing %s", old.path)
						if err := os.Remove(source); err != nil {
							h.errs.Send(err)
						}
					}(old.path, old.done)
				}
				h.mu.Unlock()
			}
		}
		log.Trace("filestorage filter loop good exit")
	}()

	if h.combinedEnabled {
		now := time.Now()
		if err := writeSingleFile(
			logFile{
				path: filenameFunc(now, h.combined),
				rx:   combineCh,
				done: nil,
			},
			*h.errs,
			h.gzip,
			context.TODO(),
			h.wg,
		); err != nil {
			return err
		}
	}

	return nil
}

func (h Handle) Errors() <-chan error {
	return h.errs.Items
}

func writeSingleFile(
	l logFile,
	errs utils.ErrChan,
	gz bool,
	ctx context.Context,
	wg *sync.WaitGroup,
) error {
	path, err := utils.ExpandHome(l.path)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	w := func() io.WriteCloser {
		if gz {
			return gzip.NewWriter(f)
		}
		return f
	}()
	wg.Add(1)
	go func() {
		defer w.Close()
		defer wg.Done()
		var written int
	loop:
		for {
			select {
			case msg, ok := <-l.rx:
				if !ok {
					break loop
				}
				fmt.Fprintf(w, "%s\n", strings.TrimRight(string(msg.Data), "\n"))
				written++
			case <-ctx.Done():
				break loop
			}
		}
		if l.done != nil {
			l.done <- true
		}
		log.Tracef("%s proper exit", path)
	}()
	return nil
}

func (h Handle) Wait() {
	h.wg.Wait()
	return
}
