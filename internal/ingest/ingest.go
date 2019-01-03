package ingest

import "github.com/ccdcoe/go-peek/internal/types"

type Ingester interface {
	types.Messager
}
