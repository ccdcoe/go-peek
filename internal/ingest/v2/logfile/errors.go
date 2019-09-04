package logfile

import "fmt"

type ErrFuncMissing struct {
	Caller, Func string
}

func (e ErrFuncMissing) Error() string {
	return fmt.Sprintf(
		"Missing function argument in %s: %s",
		e.Caller,
		e.Func,
	)
}
