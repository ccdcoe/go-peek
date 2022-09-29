package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"go-peek/pkg/models/consumer"

	olivere "github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
)

var (
	DefaultBulkFlushInterval = 10 * time.Second
	TimeFmt                  = "2006.01.02"
)

var (
	ErrMissingStream = errors.New("Missing input stream")
	ErrMissingMapFn  = errors.New("Missing index name mapping function")
)

type Config struct {
	Workers  int
	Interval time.Duration
	Hosts    []string
	Debug    bool
	Stream   <-chan consumer.Message
	Logger   *logrus.Logger
	Fn       consumer.TopicMapFn

	Username, Password string
}

func NewDefaultConfig() *Config {
	return &Config{
		Workers:  1,
		Interval: DefaultBulkFlushInterval,
		Hosts: []string{
			"http://localhost:9200",
		},
	}
}

// Validate should give an error if config is invalid, but that leads to OOP hell
// Just set default params if wonky
func (c *Config) Validate() error {
	if c == nil {
		c = NewDefaultConfig()
	}
	if c.Workers < 1 {
		c.Workers = 1
	}
	if c.Interval == 0 {
		c.Interval = DefaultBulkFlushInterval
	}
	if c.Hosts == nil || len(c.Hosts) == 0 {
		c.Hosts = []string{
			"http://localhost:9200",
		}
	}
	return nil
}

// Handle is a wrapper around olivere Bulk indexing service for my use-case
// so I don't need to manage client, bulks, service, etc
// designed to operate on a stream of log events where each message is committed to configured elastic instance
type Handle struct {
	indexer *olivere.BulkProcessor
	client  *olivere.Client
	active  bool
	RX      <-chan consumer.Message
	Fn      consumer.TopicMapFn
	Logger  *logrus.Logger
}

func NewHandle(c *Config) (*Handle, error) {
	if c == nil {
		c = NewDefaultConfig()
	}
	if c.Logger != nil {
		c.Logger.Tracef("Elastic libary version %s", olivere.Version)
	}

	var client *olivere.Client
	var err error
	if c.Username != "" && c.Password != "" {
		client, err = olivere.NewClient(
			olivere.SetURL(c.Hosts...),
			olivere.SetSniff(false),
			olivere.SetHealthcheckInterval(10*time.Second),
			olivere.SetHealthcheckTimeout(5*time.Second),
			olivere.SetBasicAuth(c.Username, c.Password),
		)
	} else {
		client, err = olivere.NewClient(
			olivere.SetURL(c.Hosts...),
			olivere.SetSniff(false),
			olivere.SetHealthcheckInterval(10*time.Second),
			olivere.SetHealthcheckTimeout(5*time.Second),
		)
	}
	if err != nil {
		return nil, err
	}
	h := &Handle{
		client: client,
	}
	b, err := client.BulkProcessor().
		Workers(c.Workers).
		BulkActions(1000).
		BulkSize(2 << 20).
		FlushInterval(c.Interval).
		Stats(true).
		Do(context.TODO())

	if err != nil {
		return h, err
	}

	h.indexer = b
	h.active = true
	h.RX = c.Stream
	h.Logger = c.Logger
	h.Fn = c.Fn

	return h, nil
}

func (h Handle) Do(ctx context.Context, wg *sync.WaitGroup) error {
	if h.RX == nil {
		return ErrMissingStream
	}
	if h.Fn == nil {
		return ErrMissingMapFn
	}
	if wg != nil {
		wg.Add(1)
	}
	go func() {
		if wg != nil {
			defer wg.Done()
		}
		defer func() {
			h.indexer.Flush()
			if h.Logger != nil {
				h.Logger.Trace("elastic producer good exit")
			}
		}()
	loop:
		for h.active {
			select {
			case msg, ok := <-h.RX:
				if !ok {
					break loop
				}
				h.indexer.Add(
					olivere.NewBulkIndexRequest().
						Index(h.Fn(msg)).
						Doc(json.RawMessage(msg.Data)),
				)
			case <-ctx.Done():
				break loop
			}
		}
	}()
	return nil
}

func (h Handle) Close() error {
	if h.indexer != nil {
		return h.indexer.Close()
	}
	return nil
}

func (b Handle) Stats() olivere.BulkProcessorStats {
	return b.indexer.Stats()
}
