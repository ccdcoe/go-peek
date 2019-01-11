package main

import (
	"container/list"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"runtime"
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
	timeFrom = mainFlags.String("time-from", "2018-12-01 00:00:00",
		`Process messages with timestamps > value. Format is YYYY-MM-DD HH:mm:ss`)
	timeTo = mainFlags.String("time-to", "2018-12-07 00:00:00",
		`Process messages with timestamps < value. Format is YYYY-MM-DD HH:mm:ss`)
)

func main() {
	mainFlags.Parse(os.Args[1:])

	start := time.Now()
	workers := runtime.NumCPU()

	files := file.ListFilesGenerator(*logdir, nil).Slice().Sort().FileListing()
	if *consume {
		out := files.ReadFiles(workers, *timeout)

		go func() {
			for err := range out.Logs.Errors() {
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s\n", err.Error())
					os.Exit(1)
				}
			}
		}()

		fmt.Fprintf(os.Stdout, "Working\n")
		from, err := time.Parse(argTsFormat, *timeFrom)
		if err != nil {
			panic(err)
		}
		to, err := time.Parse(argTsFormat, *timeTo)
		if err != nil {
			panic(err)
		}
		if from.Nanosecond() > to.Nanosecond() {
			panic("from > to")
		}

		splitter := make(map[string]chan types.Message)
		for _, v := range files {
			splitter[v.Path] = make(chan types.Message)
		}
		go func() {
			var wg sync.WaitGroup
			for _, v := range splitter {
				go func(ch chan types.Message) {
					defer wg.Done()
					times := list.New()
					for msg := range ch {
						times.PushBack(msg.Time)
					}
				}(v)
			}
			wg.Wait()
		}()

		safety := make(chan types.Message, 0)
		go func() {
			var wg sync.WaitGroup
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
								Time:   TimeEvent.GetEventTime(),
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

	} else {
		err := <-files.StatFiles(workers, *timeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(1)
		}
	}
	took := time.Since(start)

	for _, v := range files {
		fmt.Fprintf(
			os.Stdout,
			"%s - %d lines - %.2f KBytes - %s perms\n",
			v.Path,
			v.Lines,
			float64(v.Size())/1024,
			v.Mode().Perm(),
		)
	}
	fmt.Fprintf(os.Stdout, "Done reading %d files, took %.3f seconds\n", len(files), took.Seconds())
}
