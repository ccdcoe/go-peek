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
	"github.com/ccdcoe/go-peek/decoder"
	"github.com/ccdcoe/go-peek/outputs"
)

func decodedMessageConsumer(
	input chan decoder.DecodedMessage,
	wg *sync.WaitGroup,
	appConfg *mainConf,
	saganChannels map[string]chan decoder.DecodedMessage,
	producer sarama.AsyncProducer,
) {
	defer wg.Done()

	var (
		send = time.NewTicker(3 * time.Second)
		ela  = outputs.NewBulk(appConfg.ElasticSearch.Output)
	)

loop:
	for {
		select {
		case msg, ok := <-input:
			if !ok {
				break loop
			}
			// Main produce
			producer.Input() <- &sarama.ProducerMessage{
				Topic:     appConfg.GetDestTopic(msg.Topic),
				Value:     sarama.ByteEncoder(msg.Val),
				Key:       sarama.ByteEncoder(msg.Key),
				Timestamp: msg.Time,
			}

			if _, ok := saganChannels[msg.Topic]; ok {
				saganChannels[msg.Topic] <- msg
			}
			ela.AddIndex(msg.Val, appConfg.GetDestTimeElaIndex(msg.Time, msg.Topic))

		case <-send.C:
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
	appConfg *mainConf,
	producerConfig *sarama.Config,
	errs chan error,
) {
	defer wg.Done()
	defer fmt.Fprintf(os.Stdout, "Sagan consumer %s done\n", id)

	var topic = appConfg.EventTypes[id].Sagan.Topic
	saganProducer, err := sarama.NewAsyncProducer(
		appConfg.EventTypes[id].Sagan.Brokers,
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
	appConfg *mainConf,
	dec *decoder.Decoder,
	errs chan error,
) {
	defer dumpNames.Stop()
	fmt.Println("starting elastic name dumper")
	elaNameDumper := outputs.NewBulk(
		appConfg.ElasticSearch.RenameMap.Hosts,
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
					appConfg.ElasticSearch.RenameMap.Index, k)
			}
			for k, v := range dec.IPmaps() {
				data, _ := json.Marshal(struct {
					Addr, Pretty string
				}{
					Addr:   k,
					Pretty: v,
				})
				elaNameDumper.AddIndex(data,
					appConfg.ElasticSearch.RenameMap.IPaddrIndex, k)
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
