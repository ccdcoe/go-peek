package outputs

import (
	"sync"
	"time"

	"github.com/ccdcoe/go-peek/internal/logging"
)

type OutputConfig struct {
	MainKafkaBrokers []string
	MainTopicMap     map[string]string

	KeepKafkaTopic bool

	FeedbackKafkaBrokers []string
	FeedbackTopicMap     map[string]string

	ElaProxies []string
	ElaFlush   time.Duration

	Logger logging.LogHandler

	Wait *sync.WaitGroup
}
