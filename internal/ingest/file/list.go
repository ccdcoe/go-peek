package file

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"
)

type FileListGenFunc func(dir string, pattern *regexp.Regexp) (out chan string)

func ListFilesGenerator(dir string, pattern *regexp.Regexp) GenChannel {
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return nil
	}
	generator := make(chan string)
	go func() {
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				if pattern != nil && pattern.MatchString(path) {
					generator <- path
				} else if pattern == nil {
					generator <- path
				}
			}
			return nil
		})
		close(generator)
	}()
	return generator
}

type GenChannel chan string

func (c GenChannel) Slice() StringListing {
	collection := []string{}
	for item := range c {
		collection = append(collection, item)
	}
	return collection
}

type StringListing []string

func (l StringListing) Sort() StringListing {
	sort.Strings(l)
	return l
}

func (l StringListing) FileListing() FileInfoListing {
	files := make([]*FileInfo, len(l))
	for i, v := range l {
		files[i] = &FileInfo{Path: v}
	}
	return files
}

type FileInfoListing []*FileInfo

func (l FileInfoListing) CountLines(workers int, timeout time.Duration) <-chan error {
	if workers < 1 {
		workers = 1
	}
	errs := make(chan error, len(l)*2)
	files := make(chan *FileInfo, 0)
	counter := CountLinesBlock

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
						lines, err := CountFromPath(f.Path, countfn)
						if err != nil {
							errs <- err
						}
						f.Lines = lines
					case <-ctx.Done():
						errs <- ctx.Err()
						break loop
					}
				}
			}(counter, &wg)
		}
		wg.Wait()
	}()

	go func() {
		defer close(files)
		for _, v := range l {
			files <- v
		}
	}()

	return errs
}

func CountFromPath(path string, countfn LineCountFunc) (int, error) {
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

type FileInfo struct {
	Lines int
	Path  string
}

type FileInfoModifyFunc func(*FileInfo) error
