package types

type ErrNotImplemented struct {
	Err error
}

func (e ErrNotImplemented) Error() string {
	return e.Err.Error()
}
