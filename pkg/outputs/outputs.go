package outputs

import (
	"context"

	"go-peek/pkg/models/consumer"
)

// Feeder is an interface for feeding a single async producer with multiple parallel event streams
// each feeder provides a single stream while multiple feeders can be active at once
// feeder should not blocking
type Feeder interface {
	Feed(<-chan consumer.Message, string, context.Context, consumer.TopicMapFn) error
}
