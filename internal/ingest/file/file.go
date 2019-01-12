package file

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/ccdcoe/go-peek/internal/logging"
	"github.com/ccdcoe/go-peek/internal/types"
)

type ErrTimeParse struct {
	err  error
	buf  []byte
	file string
}

func (e ErrTimeParse) Error() string {
	return fmt.Sprintf("%s %s %s", e.file, string(e.buf), e.err.Error())
}

type LogFileTimeRange struct {
	From, To time.Time
}
type LogFileTimeParseFunc func([]byte, *LogFile) (time.Time, error)

type LogFile struct {
	Lines int64
	Path  string

	Empty bool
	Ctx   context.Context

	os.FileInfo
	LogFileTimeRange
}

func (f *LogFile) Stat() error {
	stat, err := os.Stat(f.Path)
	f.FileInfo = stat
	return err
}

func (f *LogFile) StartTime(parser LogFileTimeParseFunc) error {
	stream, err := OpenFile(f.Path)
	if err != nil {
		return err
	}
	defer stream.Close()
	first, err := FileReadFirstLine(stream)
	if err != nil {
		return err
	}
	ts, err := parser(first, f)
	if err != nil {
		return &ErrTimeParse{
			err:  err,
			buf:  first,
			file: f.Path,
		}
	}
	if ts.UnixNano() == 0 {
		return fmt.Errorf("Invalid first timestamp from logstream %s", ts.String())
	}
	f.From = ts
	return nil
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
func (f LogFile) AsyncRead(messages chan<- types.Message) <-chan error {
	out := make(chan error)
	go func() {
		defer close(out)
		out <- f.Read(messages)
	}()
	return out
}

type LogFileChan chan *LogFile

func (g LogFileChan) Slice() FileInfoListing {
	files := make([]*LogFile, 0)
	for v := range g {
		files = append(files, v)
	}
	return files
}

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
	return l.work(workers, timeout, CountLinesBlock, nil, nil)
}
func (l FileInfoListing) ReadFiles(workers int, timeout time.Duration) LineReadOut {
	output := &LineReadOut{
		out:  make(chan types.Message, 0),
		Logs: logging.NewLogHandler(),
	}
	errs := l.work(workers, timeout, nil, output, nil)
	go func() {
		defer close(output.out)
		for err := range errs {
			output.Logs.Error(err)
		}
	}()

	return *output
}
func (l FileInfoListing) StartTimes(
	workers int,
	timeout time.Duration,
	fn LogFileTimeParseFunc,
) <-chan error {
	return l.work(workers, timeout, nil, nil, fn)
}
func (l FileInfoListing) SortByTime() FileInfoListing {
	sort.Slice(l, func(i, j int) bool {
		return l[i].From.UnixNano() < l[j].From.UnixNano()
	})
	return l
}
func (l FileInfoListing) Prune(from, to time.Time, purgeEmpty bool) FileInfoListing {
	newlist := make(FileInfoListing, 0)
	for _, v := range l {
		if v.From.UnixNano() < from.UnixNano() {
			continue
		}
		if v.To.UnixNano() > to.UnixNano() {
			continue
		}
		if purgeEmpty && v.Empty {
			continue
		}
		newlist = append(newlist, v)
	}
	return newlist
}
func (l FileInfoListing) Raw() []*LogFile {
	return []*LogFile(l)
}
func (l FileInfoListing) Map() map[string]*LogFile {
	out := make(map[string]*LogFile)
	for _, v := range l {
		out[v.Path] = v
	}
	return out
}
func (l FileInfoListing) Get(name string) *LogFile {
	for _, v := range l {
		if v.Path == name {
			return v
		}
	}
	return nil
}

func (l FileInfoListing) work(
	workers int,
	timeout time.Duration,
	lnCntFn LineCountFunc,
	lnRdCnf *LineReadOut,
	frstTimeFn LogFileTimeParseFunc,
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
						if f.Empty {
							// *TODO* notify instead of silent skip
							continue loop
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
							if lines == 0 {
								f.Empty = true
							}
							f.Lines = lines
						}

						if frstTimeFn != nil {
							if err := f.StartTime(frstTimeFn); err != nil {
								if err == io.EOF {
									f.Empty = true
								} else {
									errs <- err
								}
							}
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
