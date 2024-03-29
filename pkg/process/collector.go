package process

import (
	"bytes"
	"errors"
)

var (
	ErrMissingHandlerFunc = errors.New("Missing handler func")
)

var newline = []byte("\n")

type CollectBulkFullFn func(*bytes.Buffer) error

type Collector struct {
	Data *bytes.Buffer
	Size int

	HandlerFunc CollectBulkFullFn
}

func (h *Collector) Collect(data []byte) error {
	if err := h.validate(); err != nil {
		return err
	}
	if len(data)+len(newline)+h.Data.Len() >= h.Size {
		if err := h.Flush(); err != nil {
			return err
		}
	}
	h.Data.Write(data)
	if !bytes.HasSuffix(data, newline) {
		h.Data.Write(newline)
	}
	return nil
}

func (h *Collector) Flush() error {
	if err := h.validate(); err != nil {
		return err
	}
	if err := h.HandlerFunc(h.Data); err != nil {
		return err
	}
	h.rotate()
	return nil
}

func (h *Collector) rotate() *Collector {
	if h.Data == nil {
		var b bytes.Buffer
		h.Data = &b
	} else {
		h.Data.Reset()
	}
	return h
}

func (h *Collector) validate() error {
	if h.HandlerFunc == nil {
		return ErrMissingHandlerFunc
	}
	if h.Data == nil {
		h.rotate()
	}
	if h.Size <= 0 {
		h.Size = 1024 * 16
	}
	return nil
}
