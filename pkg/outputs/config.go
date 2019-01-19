package outputs

import (
	"sync"
	"time"

	"github.com/ccdcoe/go-peek/internal/logging"
)

type OutputConfig struct {
	MainKafkaBrokers []string
	TopicMap         OutputTopicConfigMap

	KeepKafkaTopic bool

	FeedbackKafkaBrokers []string

	ElaProxies []string
	ElaFlush   time.Duration

	Logger logging.LogHandler

	Wait *sync.WaitGroup
}

func (c OutputConfig) SaganSet() map[string]bool {
	var set = map[string]bool{}
	if c.TopicMap != nil && len(c.TopicMap) > 0 {
		for k, v := range c.TopicMap {
			if v.SaganFormat {
				set[k] = true
			}
		}
	}
	return set
}

type OutputTopicConfig struct {
	Name        string
	Topic       string
	SaganFormat bool
}

type OutputTopicConfigMap map[string]OutputTopicConfig

func (tm OutputTopicConfigMap) MapSources(sourcemap map[string]string) OutputTopicConfigMap {
	confmap := map[string]OutputTopicConfig{}
	for k, v := range sourcemap {
		confmap[k] = OutputTopicConfig{
			Name:        tm[v].Name,
			Topic:       tm[v].Topic,
			SaganFormat: tm[v].SaganFormat,
		}
	}
	return confmap
}

type OutputElaBulkConfig struct {
	Proxies []string
	Flush   time.Duration
}
