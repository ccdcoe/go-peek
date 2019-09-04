package replay

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/ccdcoe/go-peek/pkg/events/v2"
	"github.com/ccdcoe/go-peek/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func dumpSequences(spooldir string, s []*Sequence) error {
	if s == nil || len(s) == 0 {
		return nil
	}
	for _, item := range s {
		if item == nil {
			continue
		}
		cacheFile, err := checkCache(item.ID(), spooldir, "sequences", JSON)
		if err != nil {
			return err
		}
		if err := dumpSequence(cacheFile, *item, JSON); err != nil {
			return err
		}
	}
	return nil
}

func dumpSequence(path string, s Sequence, f Format) error {
	switch f {
	case JSON:
		data, err := json.Marshal(s)
		if err != nil {
			return err
		}
		if err := ioutil.WriteFile(path, data, 0640); err != nil {
			return err
		}
	case Gob:
		if err := utils.GobSaveFile(path, s); err != nil {
			return err
		}
	}
	return nil
}

func checkCache(id, spooldir, subdir string, f Format) (string, error) {
	cacheFile, err := cacheFile(id, spooldir, subdir, f)
	if err != nil {
		return cacheFile, err
	}
	dir := filepath.Dir(cacheFile)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.Mkdir(dir, 0750)
	}
	return cacheFile, err
}

func cacheFile(id, dir, sub string, f Format) (string, error) {
	var err error
	if dir, err = utils.ExpandHome(dir); err != nil {
		return dir, err
	}
	dir = path.Join(dir, sub)
	return path.Join(dir, fmt.Sprintf("%s%s", id, f.ext())), nil
}

// ParseJSONTime implements utils.StatFileIntervalFunc
// events and ingest libraries are not supposed to know each other implementations as log stream may come in variety of different formats
// thus, interface functions are used as arguments when running tasks such as parsing first and last timestamps
func getIntervalFromJSON(first, last []byte) (utils.Interval, error) {
	var (
		i    = &utils.Interval{}
		b, e events.KnownTimeStamps
	)
	if err := json.Unmarshal(first, &b); err != nil {
		return *i, err
	}
	i.Beginning = b.Time()
	if err := json.Unmarshal(last, &e); err != nil {
		return *i, err
	}
	i.End = e.Time()
	return *i, nil
}

func storeOrLoadCache(h *Handle, spooldir string) error {
	cacheFile, err := checkCache(h.ID(), spooldir, "cache", Gob)
	if err != nil {
		return err
	}

	if utils.FileNotExists(cacheFile) {

		if err := h.build(); err != nil {
			return err
		}
		if err := utils.GobSaveFile(cacheFile, *h); err != nil {
			return err
		}
	} else {

		if err := utils.GobLoadFile(cacheFile, h); err != nil {
			return err
		}
		contextLog := log.WithFields(log.Fields{
			"file":  cacheFile,
			"dir":   filepath.Dir(cacheFile),
			"lines": h.Lines,
			"diffs": len(h.Diffs),
		})
		contextLog.Trace("loaded cache file")
	}

	return nil
}
