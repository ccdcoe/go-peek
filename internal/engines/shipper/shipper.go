package shipper

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	"github.com/ccdcoe/go-peek/pkg/outputs/elastic"
	"github.com/ccdcoe/go-peek/pkg/outputs/filestorage"
	"github.com/ccdcoe/go-peek/pkg/outputs/kafka"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func Send(
	msgs <-chan *consumer.Message,
) error {
	// TODO - move code to internal/output or something, wrap for reusability in multiple subcommands
	var (
		stdout      = viper.GetBool("output.stdout")
		fifoPaths   = viper.GetStringSlice("output.fifo.path")
		fifoEnabled = func() bool {
			if !viper.GetBool("output.fifo.enabled") {
				return false
			}
			if fifoPaths == nil || len(fifoPaths) == 0 {
				return false
			}
			return true
		}()
		elaEnabled   = viper.GetBool("output.elastic.enabled")
		kafkaEnabled = viper.GetBool("output.kafka.enabled")
		fileEnabled  = viper.GetBool("output.file.enabled")
	)

	if !stdout && !fifoEnabled && !elaEnabled && !kafkaEnabled && !fileEnabled {
		log.Fatal("No outputs configured. See --help.")
	}

	bufsize := 0
	stdoutCh := make(chan consumer.Message, bufsize)
	fifoCh := func() []chan consumer.Message {
		chSl := make([]chan consumer.Message, len(fifoPaths))
		for i := range fifoPaths {
			chSl[i] = make(chan consumer.Message, bufsize)
		}
		return chSl
	}()
	elaCh := make(chan consumer.Message, bufsize)
	kafkaCh := make(chan consumer.Message, bufsize)
	fileCh := make(chan consumer.Message, bufsize)

	defer func() {
		close(fileCh)
		log.Trace("closing filestorage channel")
	}()
	defer close(stdoutCh)
	defer func() {
		for _, ch := range fifoCh {
			close(ch)
		}
	}()
	defer close(elaCh)
	defer close(kafkaCh)

	if kafkaEnabled {
		var fn consumer.TopicMapFn
		prefix := viper.GetString("output.kafka.prefix")
		if viper.GetBool("output.kafka.merge") {
			fn = func(msg consumer.Message) string {
				return prefix
			}
		} else if topic := viper.GetString("output.kafka.topic"); topic != "" {
			fn = func(msg consumer.Message) string {
				return topic
			}
		} else {
			fn = func(msg consumer.Message) string {
				return fmt.Sprintf("%s-%s", prefix, func() string {
					if msg.Key == "" {
						return "bogon"
					}
					return msg.Key
				}())
			}
		}

		kafkaProducer, err := kafka.NewProducer(&kafka.Config{Brokers: viper.GetStringSlice("output.kafka.host")})
		if err != nil {
			log.WithFields(log.Fields{
				"hosts": viper.GetStringSlice("output.kafka.host"),
			}).Fatal(err)
		}
		kafkaProducer.Feed(kafkaCh, "replay", context.Background(), fn)
		// TODO - better producer error handler, but panic is overkill for now
		go func() {
			every := time.NewTicker(1 * time.Second)
			for {
				select {
				case <-every.C:
					if err := kafkaProducer.Errors(); err != nil {
						log.Error(err)
					}
				}
			}
		}()
	}

	if elaEnabled {
		var fn consumer.TopicMapFn
		prefix := viper.GetString("output.elastic.prefix")
		if viper.GetBool("output.elastic.merge") {
			fn = func(msg consumer.Message) string {
				return fmt.Sprintf(
					"%s-%s",
					prefix, func() time.Time {
						if msg.Time.IsZero() {
							return time.Now()
						}
						return msg.Time
					}().Format(elastic.TimeFmt))
			}
		} else {
			fn = func(msg consumer.Message) string {
				return fmt.Sprintf(
					"%s-%s-%s", prefix, func() string {
						if msg.Key == "" {
							return "bogon"
						}
						return msg.Key
					}(), func() time.Time {
						if msg.Time.IsZero() {
							return time.Now()
						}
						return msg.Time
					}().Format(elastic.TimeFmt))
			}
		}

		ela, err := elastic.NewHandle(&elastic.Config{
			Workers:  viper.GetInt("output.elastic.threads"),
			Interval: 5 * time.Second,
			Hosts:    viper.GetStringSlice("output.elastic.host"),
		})
		if err != nil {
			log.WithFields(log.Fields{
				"hosts": viper.GetStringSlice("output.elastic.host"),
			}).Fatal(err)
		}
		ela.Feed(elaCh, "replay", context.Background(), fn)
	}

	if stdout {
		log.Info("stdout enabled, starting handler")
		go func(rx <-chan consumer.Message) {
			for msg := range rx {
				fmt.Fprintf(os.Stdout, "%s\n", string(msg.Data))
			}
		}(stdoutCh)
	}

	if fifoEnabled {
		for i, pth := range fifoPaths {
			log.Infof("fifo enabled, starting handler for %s", pth)
			// TODO - this blocks when no readers
			pipe, err := os.OpenFile(pth, os.O_RDWR, os.ModeNamedPipe)
			if err != nil {
				log.Fatal(err)
			}
			defer pipe.Close()
			go func(rx <-chan consumer.Message) {
				for msg := range rx {
					fmt.Fprintf(pipe, "%s\n", string(msg.Data))
				}
			}(fifoCh[i])
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	if fileEnabled {
		writer, err := filestorage.NewHandle(&filestorage.Config{
			Dir:            viper.GetString("output.file.dir"),
			Combined:       viper.GetString("output.file.path"),
			Gzip:           viper.GetBool("output.file.gzip"),
			Timestamp:      viper.GetBool("output.file.timestamp"),
			RotateEnabled:  viper.GetBool("output.file.rotate.enabled"),
			RotateInterval: viper.GetDuration("output.file.rotate.interval"),
			Stream:         fileCh,
		})
		if err != nil {
			log.Fatal(err)
		}
		if err := writer.Do(ctx); err != nil {
			log.Fatal(err)
		}
		defer writer.Wait()
		go func() {
			for err := range writer.Errors() {
				log.Error(err)
			}
		}()
	}

	for m := range msgs {
		if stdout {
			stdoutCh <- *m
		}
		if fifoEnabled {
			for _, tx := range fifoCh {
				tx <- *m
			}
		}
		if elaEnabled {
			elaCh <- *m
		}
		if kafkaEnabled {
			kafkaCh <- *m
		}
		if fileEnabled {
			fileCh <- *m
		}
	}
	cancel()
	return nil
}
