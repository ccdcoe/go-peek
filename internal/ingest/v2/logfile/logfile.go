package logfile

import (
	"fmt"
	"sync"

	"github.com/ccdcoe/go-peek/pkg/utils"
)

var lineSep = []byte{'\n'}

type ErrMissingParam struct {
	Param, Func string
}

func (e ErrMissingParam) Error() string {
	return fmt.Sprintf("Missing param %s for function %s", e.Param, e.Func)
}

func AsyncStatAll(
	root string,
	fn utils.StatFileIntervalFunc,
	workers int,
) ([]*Handle, error) {
	files, err := genFileList(root, false)
	if err != nil {
		return nil, err
	}
	if workers < 1 {
		workers = 1
	}

	var (
		rx   = make(chan Path, 0)
		tx   = make(chan *Handle, 0)
		errs = make(chan error, len(files))
	)

	var wg sync.WaitGroup
	go func() {
		defer close(tx)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for f := range rx {
					s, err := newHandle(f, fn)
					if err != nil {
						errs <- err
					}
					tx <- s
				}
			}()
		}
		wg.Wait()
	}()

	go func() {
		for _, f := range files {
			rx <- f
		}
		close(rx)
	}()

	out := make([]*Handle, 0)
	for item := range tx {
		out = append(out, item)
	}
	if len(errs) > 0 {
		return out, utils.ErrChan{
			Desc:  "Async logfile stat",
			Items: errs,
		}
	}
	close(errs)
	return out, nil
}
