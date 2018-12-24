package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/ccdcoe/go-peek/internal/config"
	"github.com/ccdcoe/go-peek/internal/decoder"
	"github.com/ccdcoe/go-peek/internal/outputs"
)

func decodedMessageConsumer(
	input chan decoder.DecodedMessage,
	wg *sync.WaitGroup,
	appConfg *config.Config,
	saganChannels map[string]chan decoder.DecodedMessage,
	producer sarama.AsyncProducer,
) {
	defer wg.Done()

	var (
		send  = time.NewTicker(3 * time.Second)
		ela   = outputs.NewBulk(appConfg.Elastic.Output)
		topic string
	)

loop:
	for {
		select {
		case msg, ok := <-input:
			if !ok {
				break loop
			}
			// Main produce
			if topic, ok = appConfg.Stream.GetTopic(msg.Topic); ok {
				producer.Input() <- &sarama.ProducerMessage{
					Topic:     topic,
					Value:     sarama.ByteEncoder(msg.Val),
					Key:       sarama.ByteEncoder(msg.Key),
					Timestamp: msg.Time,
				}

				// Send to sagan if needed
				if _, ok := saganChannels[msg.Topic]; ok {
					saganChannels[msg.Topic] <- msg
				}

				// Collect ela bulk
				ela.AddIndex(msg.Val, appConfg.Stream.ElaIdx(msg.Time, msg.Topic))
			}

		case <-send.C:
			// Periodic ela bulk flush
			ela.Flush()
		}
	}
	fmt.Println("Message consumer done")
	for k := range saganChannels {
		close(saganChannels[k])
	}
}

func saganProducer(
	id string,
	input chan decoder.DecodedMessage,
	wg *sync.WaitGroup,
	appConfg *config.Config,
	producerConfig *sarama.Config,
	errs chan error,
) {
	defer wg.Done()
	defer fmt.Fprintf(os.Stdout, "Sagan consumer %s done\n", id)

	var topic, _ = appConfg.Stream.GetSaganTopic(id)
	saganProducer, err := sarama.NewAsyncProducer(
		appConfg.Stream[id].Sagan.Brokers,
		producerConfig)

	if err != nil {
		printErr(err)
		errs <- err
	}
	defer saganProducer.Close()

loop:
	for {
		select {
		case msg, ok := <-input:
			if !ok {
				break loop
			}
			if msg.Sagan == "" || msg.Sagan == "NOT IMPLEMENTED" {
				continue loop
			}
			// Sagan produce
			saganProducer.Input() <- &sarama.ProducerMessage{
				Topic:     topic,
				Value:     sarama.StringEncoder(msg.Sagan),
				Key:       sarama.ByteEncoder(msg.Key),
				Timestamp: msg.Time,
			}
		}
	}
}

func dumpNames(
	dumpNames *time.Ticker,
	appConfg *config.Config,
	dec *decoder.Decoder,
	errs chan error,
) {
	defer dumpNames.Stop()
	fmt.Println("starting elastic name dumper")
	elaNameDumper := outputs.NewBulk(
		appConfg.Elastic.RenameMap.Hosts,
	)
loop:
	for {
		select {
		case _, ok := <-dumpNames.C:
			if !ok {
				break loop
			}
			for k, v := range dec.Names() {
				data, _ := json.Marshal(struct {
					Original, Pretty string
				}{
					Original: k,
					Pretty:   v,
				})
				elaNameDumper.AddIndex(data,
					appConfg.Elastic.RenameMap.HostIndex, k)
			}
			for k, v := range dec.IPmaps() {
				data, _ := json.Marshal(struct {
					Addr, Pretty string
				}{
					Addr:   k,
					Pretty: v,
				})
				elaNameDumper.AddIndex(data,
					appConfg.Elastic.RenameMap.AddrIndex, k)
			}
			elaNameDumper.Flush()
		}
	}
	fmt.Println("elastic name dumper done")
}

func logErrs(
	sample int,
	consumeErrs <-chan error,
	produceErrs <-chan *sarama.ProducerError,
	decodeErrs <-chan error,
	mainErrs <-chan error,
) {
	var errcounts [4]int
	for {
		select {
		case err, ok := <-consumeErrs:
			// Handle input errors
			if ok {
				if sample < 1 || errcounts[0]%sample == 0 {
					log.Printf("ERROR [consumer]: %s\n", err.Error())
				}
				errcounts[0]++
			}
		case err, ok := <-produceErrs:
			// Handle output errors
			if ok {
				if sample < 1 || errcounts[1]%sample == 0 {
					log.Printf("ERROR [producer]: %s\n", err.Error())
				}
				errcounts[1]++
			}
		case err, ok := <-decodeErrs:
			// Handle decoder errors
			if ok {
				if sample < 1 || errcounts[2]%sample == 0 {
					// *TODO*
					// type switch to handle different scenarios
					log.Printf("Decode error: %s\n", err.Error())
				}
				errcounts[2]++
			}
		case err, ok := <-mainErrs:
			if ok {
				if sample < 1 || errcounts[3]%sample == 0 {
					log.Printf("Main error: %s\n", err.Error())
				}
				errcounts[3]++
			}
		}
	}
}

func logNotifications(
	sample int,
	decodeN <-chan string,
	consumeN <-chan *cluster.Notification,
) {
	var notifycounts int
	for {
		select {
		case msg, ok := <-decodeN:
			if ok {
				if sample < 1 || notifycounts%sample == 0 {
					log.Printf("Decode Info: %s\n", msg)
				}
				notifycounts++
			}
		case ntf, ok := <-consumeN:
			if ok {
				log.Printf("Sarama cluster rebalance: %+v\n", ntf)
			}
		}
	}
}
