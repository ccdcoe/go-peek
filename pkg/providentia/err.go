package providentia

import "fmt"

type ErrMissingInstances struct {
	Items []Target
}

func (e ErrMissingInstances) Error() string {
	return fmt.Sprintf("missing instance info on %d targets", len(e.Items))
}
