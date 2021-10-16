package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"go-peek/internal/app"
	"go-peek/pkg/models/consumer"
	"go-peek/pkg/outputs/kafka"
	"io"
	"os"
	"sync"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/sirupsen/logrus"
)

var (
	FlagKafkaBroker = flag.String("kafka-broker", "localhost:9092", "SINGLE kafka broker for bootstrap")
	FlagKafkaTopic  = flag.String("kafka-topic", "", "kafka destination topic")
	FlagInputFile   = flag.String("input-file", "", "input file path")
	FlagInputGzip   = flag.Bool("input-gzip", false, "decode input as gzip handle")
	FlagRateLimit   = flag.Bool("rate-limit", false, "rate limit producer to 1 EPS")
)

var logger = logrus.New()

func main() {
	defer app.Catch(logger)
	flag.Parse()

	if *FlagKafkaTopic == "" {
		app.Throw("setup", fmt.Errorf("missing kafka topic"))
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	producer, err := kafka.NewProducer(&kafka.Config{
		Brokers:      []string{*FlagKafkaBroker},
		AutoClose:    true,
		Logger:       logger,
		SaramaConfig: kafka.NewDefaultConfig().SaramaConfig,
	})
	app.Throw("setup", err)
	defer producer.Close()

	tx := make(chan consumer.Message, 0)
	defer close(tx)
	producer.Feed(tx, "simple producer", context.TODO(), func(m consumer.Message) string {
		return *FlagKafkaTopic
	}, &wg)

	var reader io.Reader
	if path := *FlagInputFile; path != "" {
		f, err := os.Open(*FlagInputFile)
		app.Throw(fmt.Sprintf("setup: file input: %s", *FlagInputFile), err)
		defer f.Close()

		if *FlagInputGzip {
			gReader, err := gzip.NewReader(bufio.NewReader(f))
			app.Throw(fmt.Sprintf("setup: gzip input: %s", *FlagInputFile), err)
			defer gReader.Close()
			reader = gReader
		} else {
			reader = bufio.NewReader(f)
		}
	} else {
		reader = bufio.NewReader(os.Stdin)
	}

	var count int
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		slc := make([]byte, len(scanner.Bytes()))
		copy(slc, scanner.Bytes())
		tx <- consumer.Message{
			Data: slc,
			Key:  "simple",
			Time: time.Now(),
		}
		if *FlagRateLimit {
			time.Sleep(500 * time.Millisecond)
		}
		count++
	}
	app.Throw("teardown", scanner.Err())
	logger.WithFields(logrus.Fields{
		"broker":   *FlagKafkaBroker,
		"topic":    *FlagKafkaTopic,
		"produced": count,
	}).Info("producer done")
}
