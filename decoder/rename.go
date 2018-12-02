package decoder

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/ccdcoe/go-peek/types"
	"github.com/ccdcoe/go-peek/utils"
)

const names = "https://raw.githubusercontent.com/hadley/data-baby-names/master/baby-names.csv"

type Dump struct {
	Dir, Names, Mappings string
}

func (d Dump) CheckDir() error {
	if stat, err := os.Stat(d.Dir); err != nil {
		return err
	} else if !stat.IsDir() {
		return fmt.Errorf("requested path %s is not directory", d.Dir)
	}
	return nil
}

func (d Dump) NamePath() string {
	return filepath.Join(d.Dir, d.Names)
}
func (d Dump) MappingPath() string {
	return filepath.Join(d.Dir, d.Mappings)
}

type Rename struct {
	NameSet *types.BoolValues
	ByName  map[string]string
	D       Dump
}

func NewRename(dumpPath string) (*Rename, error) {
	var (
		r       *Rename
		d       Dump
		resp    *http.Response
		err     error
		nameset = map[string]bool{}
	)
	d = Dump{
		Dir:      dumpPath,
		Names:    "names.gob",
		Mappings: "mappings.gob",
	}
	if err = d.CheckDir(); err != nil {
		return nil, err
	}
	r = &Rename{
		ByName: make(map[string]string),
		D:      d,
	}
	if _, err = os.Stat(r.D.MappingPath()); os.IsNotExist(err) {
		if resp, err = http.Get(names); err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if nameset, err = NameSetFromCSV(resp.Body); err != nil {
			return nil, err
		}
		r.NameSet = types.NewBoolValues(nameset)

		if err = utils.GobSaveFile(r.D.MappingPath(), nameset); err != nil {
			return nil, err
		}
	} else {
		var decodedMap map[string]bool
		if err := utils.GobLoadFile(r.D.MappingPath(), &decodedMap); err != nil {
			return nil, err
		}
		r.NameSet = types.NewBoolValues(decodedMap)
	}

	return r, nil
}

func (r Rename) DumpNameSet() error {
	return utils.GobSaveFile(r.D.MappingPath(), Names{V: r.NameSet.RawValues()})
}

func (r *Rename) Check(name string) string {
	if val, ok := r.ByName[name]; ok {
		return val
	}
	val := r.NameSet.GetRandom(true)
	r.ByName[name] = val
	return val
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
