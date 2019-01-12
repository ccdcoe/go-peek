package file

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
)

type FileListGenFunc func(dir string, pattern *regexp.Regexp) (out chan string)

func ListFilesGenerator(dir string, pattern *regexp.Regexp) (LogFileChan, error) {
	return ListFilesIoutilGenerator(dir, pattern)
}

func ListFilesWalkGenerator(dir string, pattern *regexp.Regexp) (LogFileChan, error) {
	if info, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%s does not exist", dir)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("%s exists but is not dir", dir)
		}
		return nil, err
	}
	generator := make(LogFileChan, 0)
	go func() {
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				if (pattern != nil && pattern.MatchString(path)) || pattern == nil {
					generator <- &LogFile{Path: path, FileInfo: info}
				}
			}
			return nil
		})
		close(generator)
	}()
	return generator, nil
}

func ListFilesIoutilGenerator(dir string, pattern *regexp.Regexp) (LogFileChan, error) {
	if info, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%s does not exist", dir)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("%s exists but is not dir", dir)
		}
		return nil, err
	}
	generator := make(LogFileChan, 0)
	errs := make(chan error, 0)
	go func() {
		defer close(generator)

		files, err := ioutil.ReadDir(dir)
		if err != nil {
			errs <- err
			return
		}
		close(errs)
		for _, info := range files {
			if !info.IsDir() {
				if (pattern != nil && pattern.MatchString(info.Name())) || pattern == nil {
					generator <- &LogFile{
						Path:     path.Join(dir, info.Name()),
						FileInfo: info,
					}
				}
			}
		}
	}()
	if err := <-errs; err != nil {
		close(errs)
		return nil, err
	}
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
