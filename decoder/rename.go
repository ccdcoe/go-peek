package decoder

import (
	"encoding/csv"
	"encoding/gob"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sync"
)

const names = "https://raw.githubusercontent.com/hadley/data-baby-names/master/baby-names.csv"

type RenameConf struct {
	GobFile, YAMLfile string
}

type Rename struct {
	mu sync.Mutex

	NameSet         map[string]bool
	ByName, BySrcIp map[string]string
	DumpPath        string
}

func NewRename(dumpfile string) (*Rename, error) {
	var (
		r    *Rename
		resp *http.Response
		err  error
	)
	if _, err = os.Stat(dumpfile); os.IsNotExist(err) {
		r = &Rename{
			ByName:   make(map[string]string),
			BySrcIp:  make(map[string]string),
			DumpPath: dumpfile,
		}
		if resp, err = http.Get(names); err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if r.NameSet, err = NameSetFromCSV(resp.Body); err != nil {
			return nil, err
		}
		if err = GobSaveFile(r.DumpPath, r); err != nil {
			return nil, err
		}
	} else {
		r = &Rename{}
		if err := GobLoadFile(dumpfile, r); err != nil {
			return nil, err
		}
	}

	return r, nil
}

func (r *Rename) Check(name string) string {
	if val, ok := r.ByName[name]; ok {
		return val
	}
	val := r.GetRandomName()
	r.mu.Lock()
	r.ByName[name] = val
	r.mu.Unlock()
	return val
}

func (r *Rename) GetRandomName() string {
	i := rand.Intn(len(r.NameSet))
	var k string
	for k = range r.NameSet {
		if i == 0 {
			break
		}
		i--
	}
	r.mu.Lock()
	delete(r.NameSet, k)
	r.mu.Unlock()
	return k
}

func NameSetFromCSV(src io.Reader) (map[string]bool, error) {
	var (
		reader  = csv.NewReader(src)
		lines   = 0
		nameSet = make(map[string]bool)
	)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		// Expects a specific CSV structure
		if record[3] == "girl" {
			nameSet[record[1]] = true
		}
		lines++
	}
	return nameSet, nil
}

func GobLoadFile(path string, object interface{}) error {
	var (
		err  error
		file io.ReadCloser
	)
	if file, err = os.Open(path); err != nil {
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	if err = decoder.Decode(object); err != nil {
		return err
	}
	return nil
}

func GobSaveFile(path string, object interface{}) error {
	var (
		err  error
		file io.WriteCloser
	)
	if file, err = os.Create(path); err != nil {
		return err
	}
	defer file.Close()

	var encoder = gob.NewEncoder(file)
	if err = encoder.Encode(object); err != nil {
		return err
	}
	return nil
}
