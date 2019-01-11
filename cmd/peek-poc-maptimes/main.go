package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"runtime"
	"time"

	"github.com/ccdcoe/go-peek/internal/ingest/file"
	"github.com/ccdcoe/go-peek/pkg/events"
)

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
)

func main() {
	mainFlags.Parse(os.Args[1:])

	start := time.Now()
	workers := runtime.NumCPU()

	files := file.ListFilesGenerator(*logdir, nil).Slice().Sort().FileListing()
	if *consume {
		out := files.ReadFiles(workers, *timeout)
		fmt.Fprintf(os.Stdout, "Printing messages\n")
		go func() {
			for err := range out.Logs.Errors() {
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s\n", err.Error())
					os.Exit(1)
				}
			}
		}()
		for msg := range out.Messages() {
			var TimeEvent events.SimpleTime
			if err := json.Unmarshal(msg.Data, &TimeEvent); err != nil {
				fmt.Fprintf(os.Stderr, "Parse error from file %s offset %d\n", msg.Source, msg.Offset)
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			}
			fmt.Fprintf(os.Stdout, "%s\n", TimeEvent.GetEventTime())
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
