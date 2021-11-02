package archive

import (
	"context"
	"errors"
	"fmt"
	"go-peek/pkg/models/consumer"
	"go-peek/pkg/utils"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	timeFmt = "20060102150405"
)

var (
	ErrMissingDir    = errors.New("Missing output folder")
	ErrMissingStream = errors.New("Missing input stream")
)

type ErrFileCreate struct {
	Err  error
	Path string
}

func (e ErrFileCreate) Error() string {
	return fmt.Sprintf("Unable to create %s: %s", e.Path, e.Err)
}

type MapFunc func(string) string

type Config struct {
	Directory      string
	Stream         <-chan consumer.Message
	RotateInterval time.Duration
	MapFunc        MapFunc
	Logger         *logrus.Logger
}

type logFile struct {
	handle       io.WriteCloser
	path         string
	created      time.Time
	writtenBytes int
}

type Handle struct {
	RX             <-chan consumer.Message
	RotateInterval time.Duration
	MapFunc        MapFunc
	Errors         chan error
	Directory      string
	Logger         *logrus.Logger
}

func (h Handle) Do(ctx context.Context, wg *sync.WaitGroup) error {
	if h.RX == nil {
		return ErrMissingStream
	}
	if wg != nil {
		wg.Add(1)
	}
	go func(rotate *time.Ticker) {
		defer rotate.Stop()
		if wg != nil {
			defer wg.Done()
		}
		var key string
		var archPath string
		writers := make(map[string]*logFile)
		defer func() {
			for _, writer := range writers {
				rotateWorker(writer.path, h.Errors, wg)
				writer.handle.Close()
			}
		}()
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case msg, ok := <-h.RX:
				if !ok {
					break loop
				}
				key = h.MapFunc(msg.Source)
				if _, ok := writers[key]; !ok {
					archPath = path.Join(h.Directory, fmt.Sprintf("%s-%s.log", key, time.Now().Format(timeFmt)))
					f, err := os.Create(archPath)
					if err != nil {
						select {
						case h.Errors <- ErrFileCreate{Err: err, Path: archPath}:
						default:
						}
						continue loop
					}
					writers[key] = &logFile{
						path:    archPath,
						created: time.Now(),
						handle:  f,
					}
					if h.Logger != nil {
						h.Logger.WithFields(logrus.Fields{
							"path": archPath,
							"key":  key,
						}).Trace("logfile created")
					}
				}
				n, _ := writers[key].handle.Write(append(msg.Data, []byte("\n")...))
				writers[key].writtenBytes += n
			case <-rotate.C:
				for key, handle := range writers {
					handle.handle.Close()
					delete(writers, key)
					rotateWorker(handle.path, h.Errors, wg)
					if h.Logger != nil {
						h.Logger.WithFields(logrus.Fields{
							"path": handle.path,
							"key":  key,
						}).Trace("logfile rotated")
					}
				}
			}
		}
	}(time.NewTicker(h.RotateInterval))
	return nil
}

func NewHandle(c Config) (*Handle, error) {
	if c.Directory == "" {
		return nil, ErrMissingDir
	}
	if stat, err := os.Stat(c.Directory); os.IsNotExist(err) {
		if err := os.Mkdir(c.Directory, 0700); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("Archive dir %s missing", c.Directory)
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("Archive dir %s exists but is not a folder", c.Directory)
	}
	h := &Handle{
		Directory: c.Directory,
		RX:        c.Stream,
		Errors:    make(chan error, 10),
		Logger:    c.Logger,
	}
	if c.RotateInterval == 0 {
		h.RotateInterval = 1 * time.Minute
	} else {
		h.RotateInterval = c.RotateInterval
	}
	if c.MapFunc == nil {
		h.MapFunc = func(s string) string { return strings.ReplaceAll(s, " ", "") }
	}
	return h, nil
}

func rotateWorker(path string, errs chan<- error, wg *sync.WaitGroup) {
	if wg != nil {
		wg.Add(1)
	}
	go func() {
		if wg != nil {
			defer wg.Done()
		}
		if !utils.ErrSendLossy(utils.GzipCompress(path, path+".gz"), errs) {
			utils.ErrSendLossy(os.Remove(path), errs)
		}
	}()
}
