package file

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
)

type FileListGenFunc func(dir string, pattern *regexp.Regexp) (out chan string)

func ListFilesGenerator(dir string, pattern *regexp.Regexp) (LogFileChan, error) {
	if info, err := os.Stat(dir); err != nil {
		if !info.IsDir() {
			return nil, fmt.Errorf("%s not dir", dir)
		}
		return nil, err
	}
	generator := make(LogFileChan, 0)
	go func() {
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				if pattern != nil && pattern.MatchString(path) {
					generator <- &LogFile{Path: path}
				} else if pattern == nil {
					generator <- &LogFile{Path: path}
				}
			}
			return nil
		})
		close(generator)
	}()
	return generator, nil
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
	files := make([]*LogFile, len(l))
	for i, v := range l {
		files[i] = &LogFile{Path: v}
	}
	return files
}
