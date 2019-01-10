package file

import (
	"context"
	"fmt"
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
	files := make(chan *FileInfo, 0)
	errs := make(chan error, len(l)*2)
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
						fmt.Println(f.Path)
						reader, err := OpenFile(f.Path)
						if err != nil {
							errs <- err
							continue loop
						}
						lines, err := countfn(reader)
						if err != nil {
							errs <- err
							continue loop
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
