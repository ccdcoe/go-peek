package replay

import (
	"crypto/sha256"
	"fmt"

	"github.com/ccdcoe/go-peek/pkg/events/v2"
	"github.com/ccdcoe/go-peek/pkg/utils"
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
	if s.Files, err = newHandleSlice(s.DataDir); err != nil {
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
