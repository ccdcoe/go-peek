package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

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

	input := *logdir
	files, err := file.ListFilesGenerator(input, nil)
	if err != nil {
		panic(err)
	}

	countfuncs := []file.LineCountFunc{
		file.CountLinesScan,
		file.CountLinesBlock,
		LineCountAndAnalyzeLast,
	}

	counts := map[string][]int64{}
	for f := range files {
		fmt.Fprintf(os.Stdout, "%s\n", f.Path)
		if _, ok := counts[f.Path]; !ok {
			counts[f.Path] = make([]int64, len(countfuncs))
		}
		for i, fn := range countfuncs {
			counts[f.Path][i] = CountLines(f.Path, fn)
		}
	}

	for k, v := range counts {
		str := make([]string, len(v))
		for i, j := range v {
			str[i] = strconv.Itoa(int(j))
		}
		fmt.Fprintf(os.Stdout, "%s : %s\n", k, strings.Join(str, "|"))
	}
}

func LineCountAndAnalyzeLast(reader io.Reader) (int64, error) {
	lines := LineR(reader)
	var count int64
	broken := 0
	for msg := range lines {
		last := string(msg[len(msg)-1])
		if last != "}" {
			broken++
		} else {
			count++
		}
	}
	return count, nil
}

func CountLines(f string, fn file.LineCountFunc) int64 {
	var (
		reader io.ReadCloser
		err    error
	)
	reader, err = file.OpenFile(f)
	if err != nil {
		panic(err)
	}
	defer reader.Close()
	count, err := fn(reader)
	if err != nil && err != io.EOF {
		panic(err)
	}
	return count
}

func LineR(file io.Reader) chan string {
	fileScanner := bufio.NewScanner(file)
	out := make(chan string, 0)

	go func() {
		defer close(out)
		for fileScanner.Scan() {
			out <- fileScanner.Text()
		}
	}()

	return out
}
