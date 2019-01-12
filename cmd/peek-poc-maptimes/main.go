package main

import (
	"container/list"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/ccdcoe/go-peek/internal/ingest/file"
	"github.com/ccdcoe/go-peek/internal/types"
	"github.com/ccdcoe/go-peek/pkg/events"
)

const argTsFormat = "2006-01-02 15:04:05"

var (
	mainFlags = flag.NewFlagSet("main", flag.ExitOnError)
	logdir    = mainFlags.String("dir", path.Join(
		os.Getenv("HOME"), "Data"),
		`Root dir for recursive logfile search`,
	)
	timeout = mainFlags.Duration("timeout", 30*time.Second,
		`Timeout for consumer`)
	consume = mainFlags.Bool("consume", false,
		`Consume messages and print to stdout, as opposed to simply statting files.`)
	timeFrom = mainFlags.String("time-from", "2018-12-30 00:00:00",
		`Process messages with timestamps > value. Format is YYYY-MM-DD HH:mm:ss`)
	timeTo = mainFlags.String("time-to", "2018-12-07 00:00:00",
		`Process messages with timestamps < value. Format is YYYY-MM-DD HH:mm:ss`)
	readers = mainFlags.Uint("readers", 4,
		`No concurrent file readers for IO operations`)
	speedup = mainFlags.Int64("ff", 1,
		`Fast forward x times`)
)

func main() {
	mainFlags.Parse(os.Args[1:])

	start := time.Now()

	workers := runtime.NumCPU()
	from, err := time.Parse(argTsFormat, *timeFrom)
	if err != nil {
		panic(err)
	}
	to, err := time.Parse(argTsFormat, *timeTo)
	if err != nil {
		panic(err)
	}
	if from.UnixNano() > to.UnixNano() {
		panic("from > to")
	}

	fileGen, err := file.ListFilesGenerator(*logdir, nil)
	if err != nil {
		panic(err)
	}
	fmt.Fprintf(os.Stdout, "Slicing files\n")
	files := fileGen.Slice()
	if err := <-files.StartTimes(workers, *timeout, func(
		line []byte,
		logfile *file.LogFile,
	) (time.Time, error) {
		var TimeEvent events.SimpleTime
		if err := json.Unmarshal(line, &TimeEvent); err != nil {
			return time.Now(), err
		}
		tme := TimeEvent.GetSyslogTime()
		return tme, nil
	}); err != nil {
		panic(err)
	}
	files = files.SortByTime().Prune(from, to, true)
	if err := <-files.StatFiles(workers, *timeout); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	if *consume {
		out := files.ReadFiles(int(*readers), *timeout)

		go func() {
			for err := range out.Logs.Errors() {
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s\n", err.Error())
					os.Exit(1)
				}
			}
		}()

		fmt.Fprintf(os.Stdout, "Working\n")

		type TimeList struct {
			Source     string
			Times      *list.List
			Start, End int64
			File       *file.LogFile
		}

		splitter := make(map[string]chan types.Message)
		linkedListOutput := make(chan *TimeList)
		for _, v := range files {
			splitter[v.Path] = make(chan types.Message)
		}
		go func() {
			defer close(linkedListOutput)
			var wg sync.WaitGroup
			for k, v := range splitter {
				wg.Add(1)
				go func(ch chan types.Message, name string) {
					defer wg.Done()
					var first, last, count int64
					var prev time.Time

					first = -1
					last = -1

					times := list.New()
					for msg := range ch {
						if msg.Time.UnixNano() > from.UnixNano() && msg.Time.UnixNano() < to.UnixNano() {
							if first < 0 {
								first = msg.Offset
							}
							if count > 0 {
								diff := msg.Time.Sub(prev)
								times.PushBack(diff)
							}
							count++
							prev = msg.Time
							last = msg.Offset
						}
					}
					linkedListOutput <- &TimeList{
						Times:  times,
						Source: name,
						Start:  first,
						End:    last,
					}
					fmt.Fprintf(os.Stdout, "Worker done %d timestamps collected\n", times.Len())
				}(v, k)
			}
			wg.Wait()
		}()

		safety := make(chan types.Message, 0)
		go func() {
			var wg sync.WaitGroup
			defer close(safety)
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for msg := range out.Messages() {
						var TimeEvent events.SimpleTime
						if err := json.Unmarshal(msg.Data, &TimeEvent); err != nil {
							fmt.Fprintf(os.Stderr, "Parse error from file %s offset %d\n", msg.Source, msg.Offset)
							fmt.Fprintf(os.Stderr, "%s\n", err.Error())
						} else {
							safety <- types.Message{
								Data:   msg.Data,
								Source: msg.Source,
								Offset: msg.Offset,
								Time:   TimeEvent.GetSyslogTime(),
							}
						}
					}
				}()
			}
			wg.Wait()
		}()

		report := time.NewTicker(1 * time.Second)
		var count, total int64
	loop:
		for {
			select {
			case safe, ok := <-safety:
				if !ok {
					break loop
				}
				splitter[safe.Source] <- safe
				count++
			case <-report.C:
				total = total + count
				fmt.Fprintf(os.Stdout, "total: %d, rate: %d m/sec\n", total, count)
				count = 0
			}
		}

		for _, v := range splitter {
			close(v)
		}

		// *TODO* Parallele topic consumption here
		fn := func(output chan types.Message, replaySlice []*TimeList, speedup int64) {
			defer close(output)

			fmt.Fprintf(os.Stdout, "Sorting files\n")
			sort.Slice(replaySlice, func(i, j int) bool {
				return replaySlice[i].File.From.UnixNano() < replaySlice[j].File.From.UnixNano()
			})

			fmt.Fprintf(os.Stdout, "Looping files\n")
		sourceLoop:
			for _, replay := range replaySlice {
				fmt.Fprintf(os.Stdout, "%s, first offset: %d last offset: %d\n",
					replay.Source, replay.Start, replay.End)
				if replay.Start < 0 || replay.End < 0 {
					fmt.Fprintf(os.Stderr, "%s has no known messages, skipping\n", replay.Source)
					continue sourceLoop
				}
				messages := make(chan types.Message)
				go func() {
					if replay.Start > 0 {
						fmt.Fprintf(os.Stdout, "%s does not start from beginning, consuming until %d\n",
							replay.Source, replay.Start)
					msgLoop:
						for msg := range messages {
							if msg.Offset == replay.Start {
								break msgLoop
							}
						}
					}
					fmt.Fprintf(os.Stderr, "Consuming\n")
					for sleep := replay.Times.Front(); sleep != nil; sleep = sleep.Next() {
						msg := <-messages
						output <- msg
						time.Sleep(sleep.Value.(time.Duration) / time.Duration(speedup))
					}
				}()

				fmt.Fprintf(os.Stdout, "Reading\n")
				if err := <-replay.File.AsyncRead(messages); err != nil {
					fmt.Fprintf(os.Stderr, "Unable to read %s. %s\n", replay.Source, err.Error())
					continue sourceLoop
				}
				close(messages)
			}
		}
		replaySlice := []*TimeList{}
		for item := range linkedListOutput {
			if logfile := files.Get(item.Source); logfile != nil {
				item.File = logfile
				replaySlice = append(replaySlice, item)
			}
		}
		if len(replaySlice) > 0 {
			fmt.Fprintf(os.Stdout, "Consuming replay")
			output := make(chan types.Message)
			go fn(output, replaySlice, *speedup)
			for msg := range output {
				fmt.Fprintf(os.Stdout, "%s\n", msg.String())
			}
		}

	}

	took := time.Since(start)
	for _, v := range files {
		fmt.Fprintf(
			os.Stdout,
			"%s - %d lines - %.2f KBytes - %s perms. Start: %s\n",
			v.Path,
			v.Lines,
			float64(v.Size())/1024,
			v.Mode().Perm(),
			v.From.Format(argTsFormat),
		)
	}

	fmt.Fprintf(os.Stdout, "Done reading %d files, took %.3f seconds\n", len(files), took.Seconds())
}
