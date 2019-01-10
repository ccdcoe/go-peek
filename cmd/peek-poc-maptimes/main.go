package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"runtime"
	"time"

	"github.com/ccdcoe/go-peek/internal/ingest/file"
)

var (
	mainFlags = flag.NewFlagSet("main", flag.ExitOnError)
	logdir    = mainFlags.String("dir", path.Join(
		os.Getenv("HOME"), "Data"),
		`Root dir for recursive logfile search`,
	)
)

func main() {
	mainFlags.Parse(os.Args[1:])

	files := file.ListFilesGenerator(*logdir, nil).Slice().Sort().FileListing()
	err := <-files.CountLines(runtime.NumCPU(), 30*time.Second)
	if err != nil {
		panic(err)
	}
	for _, v := range files {
		fmt.Fprintf(os.Stdout, "%s - %d\n", v.Path, v.Lines)
	}
}
