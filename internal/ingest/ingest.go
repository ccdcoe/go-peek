package ingest

import (
	"github.com/ccdcoe/go-peek/internal/ingest/message"
)

type Ingester interface {
	Messager
}

type Messager interface {
	Messages() <-chan message.Message
}
