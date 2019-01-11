package file

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ccdcoe/go-peek/internal/logging"
	"github.com/ccdcoe/go-peek/internal/types"
)

type LogFileTimeRange struct {
	From, To time.Time
}

type LogFile struct {
	Lines int64
	Path  string
	os.FileInfo
	LogFileTimeRange
}

func (f *LogFile) Stat() error {
	stat, err := os.Stat(f.Path)
	f.FileInfo = stat
	return err
}

func (f *LogFile) Read(messages chan<- types.Message) error {
	stream, err := OpenFile(f.Path)
	if err != nil {
		return err
	}
	defer stream.Close()
	var offset int64
	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		messages <- types.Message{
			Source: f.Path,
			Offset: offset,
			Data:   []byte(scanner.Text()),
		}
		offset++
	}
	f.Lines = offset
	return scanner.Err()
}

type FileInfoModifyFunc func(*LogFile) error
type FileReadFunc func(LogFile) chan types.Message

type LineReadOut struct {
	out  chan types.Message
	Logs logging.LogHandler
}

func (r *LineReadOut) Validate() error {
	if r.out == nil {
		return fmt.Errorf("File reader output missing")
	}
	if r.Logs == nil {
		r.Logs = logging.NewLogHandler()
		r.Logs.Notify(fmt.Sprintf("Missing log handle for file read. Creating new."))
	}
	return nil
}

func (l *LineReadOut) Messages() <-chan types.Message {
	return l.out
}

type FileInfoListing []*LogFile

func (l FileInfoListing) StatFiles(workers int, timeout time.Duration) <-chan error {
	return l.work(workers, timeout, CountLinesBlock, nil)
}
func (l FileInfoListing) ReadFiles(workers int, timeout time.Duration) LineReadOut {
	output := &LineReadOut{
		out:  make(chan types.Message, 0),
		Logs: logging.NewLogHandler(),
	}
	errs := l.work(workers, timeout, nil, output)
	go func() {
		defer close(output.out)
		for err := range errs {
			output.Logs.Error(err)
		}
	}()

	return *output
}

func (l FileInfoListing) work(
	workers int,
	timeout time.Duration,
	lnCntFn LineCountFunc,
	lnRdCnf *LineReadOut,
) <-chan error {
	if workers < 1 {
		workers = 1
	}
	errs := make(chan error, len(l)*2)
	files := make(chan *LogFile, 0)
	ctx, globalCancel := context.WithCancel(context.Background())

	go func(ctx context.Context) {
		defer close(files)
	loop:
		for _, v := range l {
			select {
			case <-ctx.Done():
				break loop
			default:
				files <- v
			}
		}
	}(ctx)

	go func() {
		var wg sync.WaitGroup
		defer close(errs)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(countfn LineCountFunc, wg *sync.WaitGroup) {
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer wg.Done()
				defer cancel()
			loop:
				for {
					select {
					case f, ok := <-files:
						if !ok {
							break loop
						}
						if err := f.Stat(); err != nil {
							errs <- err
							continue loop
						}

						if lnCntFn != nil {
							lines, err := CountFromPath(f.Path, countfn)
							if err != nil {
								errs <- err
							}
							f.Lines = lines
						}

						if lnRdCnf != nil {
							if err := lnRdCnf.Validate(); err != nil {
								errs <- err
								globalCancel()
								break loop
							} else {
								if err := f.Read(lnRdCnf.out); err != nil {
									errs <- err
								}
							}
						}
					case <-ctx.Done():
						errs <- ctx.Err()
						globalCancel()
					}
				}
			}(lnCntFn, &wg)
		}
		wg.Wait()
	}()

	return errs
}

func CountFromPath(path string, countfn LineCountFunc) (int64, error) {
	reader, err := OpenFile(path)
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	lines, err := countfn(reader)
	if err != nil {
		return lines, err
	}
	return lines, nil
}
