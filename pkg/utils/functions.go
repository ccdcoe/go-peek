package utils

import "github.com/ccdcoe/go-peek/pkg/models/events"

type EventMapFn func(string) events.Atomic
