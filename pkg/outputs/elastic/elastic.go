package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	feeders *sync.WaitGroup
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
		client:  client,
		feeders: &sync.WaitGroup{},
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

func (h Handle) add(item []byte, idx string) {
	h.indexer.Add(
		olivere.NewBulkIndexRequest().
			Index(idx).
			Doc(json.RawMessage(item)),
	)
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
				h.add(msg.Data, h.Fn(msg))
			case <-ctx.Done():
				break loop
			}
		}
	}()
	return nil
}

// Feed implements outputs.Feeder
func (h Handle) Feed(
	rx <-chan consumer.Message,
	name string,
	ctx context.Context,
	fn consumer.TopicMapFn,
) error {
	if !h.active {
		return fmt.Errorf(
			"elastic bulk handler is not active, cannot feed to base index %s",
			name,
		)
	}
	if rx == nil {
		return fmt.Errorf(
			"missing channel, cannot feed elastic bulk indexer with %s",
			name,
		)
	}

	if fn == nil {
		fn = func(consumer.Message) string {
			return fmt.Sprintf(
				"%s-%s",
				name,
				time.Now().Format(TimeFmt),
			)
		}
	}

	h.feeders.Add(1)
	go func(ctx context.Context) {
		defer h.feeders.Done()
	loop:
		for h.active {
			select {
			case msg, ok := <-rx:
				if !ok {
					break loop
				}
				h.add(msg.Data, fn(msg))
			case <-ctx.Done():
				break loop
			}
		}
		// TODO: might be better handled elsewhere
		h.indexer.Flush()
	}(func() context.Context {
		if ctx == nil {
			return context.Background()
		}
		return ctx
	}())
	return nil
}

// TODO - Close and Wait are not handled well atm

// Wait wraps around sync.WaitGroup to properly wait all bulk feeders to finish their work
// then flush the tail of message bulk
func (h Handle) Wait() {
	if h.feeders == nil {
		return
	}
	h.feeders.Wait()
}

func (h Handle) Close() error {
	if h.indexer == nil {
		return fmt.Errorf("unable to close elastic bulk indexer")
	}
	// may already be handled by return statement
	h.indexer.Flush()
	return h.indexer.Close()
}

func (b Handle) Stats() olivere.BulkProcessorStats {
	return b.indexer.Stats()
}
