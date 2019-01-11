package file

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
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
	files := make([]*LogFile, len(l))
	for i, v := range l {
		files[i] = &LogFile{Path: v}
	}
	return files
}
