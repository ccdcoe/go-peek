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

type ErrNoOutputs struct {
	Name string
}

func (e ErrNoOutputs) Error() string {
	return fmt.Sprintf("No outputs for %s module. See --help.", e.Name)
}

func Send(
	msgs <-chan *consumer.Message,
	module string,
) error {
	// TODO - move code to internal/output or something, wrap for reusability in multiple subcommands
	var (
		stdout      = viper.GetBool(module + ".stdout")
		fifoPaths   = viper.GetStringSlice(module + ".fifo.path")
		fifoEnabled = func() bool {
			if !viper.GetBool(module + ".fifo.enabled") {
				return false
			}
			if fifoPaths == nil || len(fifoPaths) == 0 {
				return false
			}
			return true
		}()
		elaEnabled   = viper.GetBool(module + ".elastic.enabled")
		kafkaEnabled = viper.GetBool(module + ".kafka.enabled")
		fileEnabled  = viper.GetBool(module + ".file.enabled")
	)

	if !stdout && !fifoEnabled && !elaEnabled && !kafkaEnabled && !fileEnabled {
		return ErrNoOutputs{Name: module}
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
		prefix := viper.GetString(module + ".kafka.prefix")
		if viper.GetBool(module + ".kafka.merge") {
			fn = func(msg consumer.Message) string {
				return prefix
			}
		} else if topic := viper.GetString(module + ".kafka.topic"); topic != "" {
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

		kafkaProducer, err := kafka.NewProducer(&kafka.Config{Brokers: viper.GetStringSlice(module + ".kafka.host")})
		if err != nil {
			log.WithFields(log.Fields{
				"hosts": viper.GetStringSlice(module + ".kafka.host"),
			}).Fatal(err)
		}
		kafkaProducer.Feed(kafkaCh, module, context.Background(), fn)
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
		prefix := viper.GetString(module + ".elastic.prefix")
		timeFmt := func() string {
			if viper.GetBool(module + ".elastic.hourly") {
				return "2006.01.02.15"
			}
			return elastic.TimeFmt
		}()
		if viper.GetBool(module + ".elastic.merge") {
			fn = func(msg consumer.Message) string {
				return fmt.Sprintf(
					"%s-%s",
					prefix, func() time.Time {
						if msg.Time.IsZero() {
							return time.Now()
						}
						return msg.Time
					}().Format(timeFmt))
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
					}().Format(timeFmt))
			}
		}

		ela, err := elastic.NewHandle(&elastic.Config{
			Workers:  viper.GetInt(module + ".elastic.threads"),
			Interval: 5 * time.Second,
			Hosts:    viper.GetStringSlice(module + ".elastic.host"),
		})
		if err != nil {
			log.WithFields(log.Fields{
				"hosts": viper.GetStringSlice(module + ".elastic.host"),
			}).Fatal(err)
		}
		ela.Feed(elaCh, module, context.Background(), fn)
		go func() {
			debug := time.NewTicker(3 * time.Second)
			for {
				select {
				case <-debug.C:
					log.Debugf("%s: %+v", module, ela.Stats())
				}
			}
		}()
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
			Dir:            viper.GetString(module + ".file.dir"),
			Combined:       viper.GetString(module + ".file.path"),
			Gzip:           viper.GetBool(module + ".file.gzip"),
			Timestamp:      viper.GetBool(module + ".file.timestamp"),
			RotateEnabled:  viper.GetBool(module + ".file.rotate.enabled"),
			RotateInterval: viper.GetDuration(module + ".file.rotate.interval"),
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
