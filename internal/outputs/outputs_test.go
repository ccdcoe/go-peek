package outputs

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ccdcoe/go-peek/internal/logging"
	"github.com/ccdcoe/go-peek/internal/types"
	"github.com/ccdcoe/go-peek/pkg/events"
)

func Generate() (<-chan types.Message, map[string]string) {
	output := make(chan types.Message)
	sources := []string{
		"source1",
		"source2",
		"source3",
	}
	maps := map[string]string{
		"source1": "topic1",
		"source2": "topic2",
		"source3": "topic3",
	}
	go func() {
		defer close(output)
		for i := 0; i < 100; i++ {
			syslog := events.NewSyslogTestMessage("")
			sagan, _ := syslog.SaganString()
			if json, err := syslog.JSON(); err == nil {
				output <- types.Message{
					Offset: int64(i),
					Data:   json,
					Time:   syslog.GetSyslogTime(),
					Source: sources[rand.Int()%len(sources)],
					Formats: map[string]string{
						"sagan": sagan,
					},
				}
			}
		}
	}()
	return output, maps
}

func TestOutput(t *testing.T) {
	fmt.Println("generating data")
	out, maps := Generate()
	o := Output(out)
	fmt.Println("creating context")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	topicmap := map[string]OutputTopicConfig{}
	for k, v := range maps {
		topicmap[k] = OutputTopicConfig{
			Topic:       v,
			SaganFormat: true,
		}
	}

	config := OutputConfig{
		MainKafkaBrokers: []string{"localhost:9092"},
		ElaProxies:       []string{"http://localhost:9200"},
		ElaFlush:         5,
		Logger:           logging.NewLogHandler(),
		Wait:             &sync.WaitGroup{},
		TopicMap:         topicmap,
		KeepKafkaTopic:   false,
	}
	go func() {
		for msg := range config.Logger.Notifications() {
			fmt.Println(msg)
		}
	}()
	fmt.Println("producing")
	if _, err := o.Produce(config, ctx); err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println("waiting")

	config.Wait.Wait()
	if len(config.Logger.Errors()) > 0 {
		err := <-config.Logger.Errors()
		t.Fatalf(err.Error())
	}
}
