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
	ByName  *types.StringValues
	BySrcIp *types.StringValues
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
	r = &Rename{D: d}
	if _, err = os.Stat(r.D.NamePath()); os.IsNotExist(err) {
		if resp, err = http.Get(names); err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if nameset, err = NameSetFromCSV(resp.Body); err != nil {
			return nil, err
		}
		r.NameSet = types.NewBoolValues(nameset)

		if err = r.SaveNames(); err != nil {
			return nil, err
		}
	} else {
		var decodedMap map[string]bool
		if err := utils.GobLoadFile(r.D.NamePath(), &decodedMap); err != nil {
			return nil, err
		}
		r.NameSet = types.NewBoolValues(decodedMap)
	}

	if _, err = os.Stat(r.D.MappingPath()); os.IsNotExist(err) {
		r.ByName = types.NewEmptyStringValues()
		if err := r.SaveMappings(); err != nil {
			return nil, err
		}
	} else {
		var decodedMap map[string]string
		if err := utils.GobLoadFile(r.D.MappingPath(), &decodedMap); err != nil {
			return nil, err
		}
		r.ByName = types.NewStringValues(decodedMap)
	}

	return r, nil
}

func (r *Rename) Check(name string) (string, bool) {
	if val, ok := r.ByName.Get(name); ok {
		return val, false
	}
	r.ByName.Lock()
	val := r.NameSet.GetRandom(true)
	r.ByName.Unlock()

	r.ByName.Set(name, val)
	return val, true
}

func (r Rename) SaveNames() error {
	return utils.GobSaveFile(r.D.NamePath(), r.NameSet.RawValues())
}

func (r Rename) SaveMappings() error {
	return utils.GobSaveFile(r.D.MappingPath(), r.ByName.RawValues())
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
