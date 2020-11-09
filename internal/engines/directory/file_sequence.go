package directory

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"

	"go-peek/pkg/ingest/logfile"
	"go-peek/pkg/models/events"
	"go-peek/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var (
	Fn      logfile.StatFileIntervalFunc
	Workers int
)

// Sequence is a container for a sequence of Handle objects (sequential log files) with attached methods and information for properly parsing and replaying the messages
// Messages in a single set should all be in same format
type Sequence struct {
	DataDir string
	Files   []*Handle
	Type    events.Atomic
}

func (s Sequence) ID() string {
	return fmt.Sprintf(
		"sequence-%x",
		sha256.Sum256([]byte(s.DataDir)),
	)
}

func (s *Sequence) Discover() (err error) {
	if s.Files, err = newHandleSlice(s.DataDir, s.Type); err != nil {
		switch v := err.(type) {
		case *utils.ErrNilPointer:
			v.Caller = v.Caller + " " + s.Type.String()
			return v
		default:
			return err
		}
	}
	return nil
}

func (s *Sequence) calcDiffsBetweenFiles() *Sequence {
	if s.Files == nil || len(s.Files) == 0 {
		return s
	}
	for _, h := range s.Files {
		if h.Interval == nil {
			return s
		}
	}
	for i, h := range s.Files {
		if i == 0 || h.Diffs == nil || len(h.Diffs) == 0 {
			continue
		}
		h.Diffs[0] = h.Interval.Beginning.Sub(s.Files[i-1].Interval.End)
	}
	return s
}

func newHandleSlice(
	dir string,
	atomic events.Atomic,
) ([]*Handle, error) {
	if Workers < 1 {
		Workers = 1
	}

	log.WithFields(log.Fields{
		"workers": Workers,
		"dir":     dir,
		"action":  "invoking async stat",
	}).Trace("replay sequence discovery")

	files, err := logfile.AsyncStatAll(dir, Fn, Workers, true, atomic)
	if err != nil {
		return nil, err
	}

	log.WithFields(log.Fields{
		"found":  len(files),
		"dir":    dir,
		"action": "sorting",
	}).Trace("discovery done")

	sort.Slice(files, func(i, j int) bool {
		return files[i].Interval.Beginning.Before(files[j].Interval.Beginning)
	})

	handles := make([]*Handle, len(files))
	for i, h := range files {
		if h == nil {
			return handles, &utils.ErrNilPointer{
				Caller:   "log file discovery",
				Function: fmt.Sprintf("log file no %d while building Handle list", i),
			}
		}
		handles[i] = &Handle{Handle: h}
	}
	return handles, nil
}

type SequenceList []*Sequence

func (s SequenceList) AsyncBuildOrLoadAll(
	spooldir string,
	cache bool,
) error {

	var (
		errs     = make(chan error, s.handleCount())
		handleCh = make(chan *Handle, 0)
		wg       sync.WaitGroup
	)

	go func() {
		wg.Add(1)
		defer wg.Done()
		defer log.Tracef("all %d timestamp collectors exited properly", Workers)

		for i := 0; i < Workers; i++ {
			wg.Add(1)

			go func(i int, rx <-chan *Handle) {
				log.Tracef("Spawning worker %d", i)

				defer wg.Done()
				defer log.Tracef("Worker %d exited", i)

				for h := range rx {

					if cache {

						log.Trace("Cache enabled")
						if err := storeOrLoadCache(h, spooldir); err != nil {
							errs <- err
						}

					} else {

						if err := h.Build(); err != nil {
							errs <- err
						}
					}
				}

			}(i, handleCh)
		}

	}()

	for _, seq := range s {
		for _, f := range seq.Files {
			handleCh <- f
		}
	}

	log.Trace("done sending handles to timestamp builders")
	close(handleCh)
	log.Trace("handle channel closed correctly")
	wg.Wait()

	if len(errs) > 0 {
		return utils.ErrChan{
			Desc:  "building from sequence list",
			Items: errs,
		}
	}
	return nil
}

func (s SequenceList) handleCount() int {
	count := 0
	for _, seq := range s {
		count += len(seq.Files)
	}
	return count
}

func (s SequenceList) SeekAll(interval utils.Interval) error {
outer:
	for _, seq := range s {
		if seq.Files == nil || len(seq.Files) == 0 {
			continue outer
		}
	inner:
		for _, h := range seq.Files {
			if h.Partial == CompletelyInRange {
				continue inner
			}
			if err := h.Seek(interval); err != nil {
				return err
			}
		}
	}
	return nil
}

// TODO: refactor
// TODO: timeParseFunc for arbitrary byte stream instead of always assuming JSON with known keys
// silent fail is best fail
func (s SequenceList) CalcDiffBeginning(i utils.Interval) SequenceList {
	if s == nil || len(s) == 0 {
		return s
	}
outer:
	for _, seq := range s {
		if seq.Files == nil || len(seq.Files) == 0 {
			continue outer
		}
	inner:
		for _, h := range seq.Files {
			if h.Enabled {
				line, err := logfile.GetLine(*h.Handle, h.Offsets.Beginning)
				if err == nil {
					var obj events.KnownTimeStamps
					if err := json.Unmarshal(line, &obj); err == nil {
						h.Diffs[h.Offsets.Beginning] = obj.Time().Sub(i.Beginning)
					}
				}
				break inner
			}
		}
	}
	return s
}

func (s SequenceList) CalcDiffsBetweenFiles() SequenceList {
	for _, seq := range s {
		seq.calcDiffsBetweenFiles()
	}
	return s
}

func storeOrLoadCache(h *Handle, spooldir string) error {
	cacheFile, err := checkCache(h.ID(), spooldir, "cache", Gob)
	if err != nil {
		return err
	}

	if utils.FileNotExists(cacheFile) {

		if err := h.Build(); err != nil {
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
func cacheFile(id, dir, sub string, f Format) (string, error) {
	var err error
	if dir, err = utils.ExpandHome(dir); err != nil {
		return dir, err
	}
	dir = path.Join(dir, sub)
	return path.Join(dir, fmt.Sprintf("%s%s", id, f.Ext())), nil
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

func DumpSequences(spooldir string, s []*Sequence) error {
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
