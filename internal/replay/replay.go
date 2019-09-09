package replay

import (
	"context"
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	"github.com/ccdcoe/go-peek/pkg/models/events"
	"github.com/ccdcoe/go-peek/pkg/outputs/elastic"
	"github.com/ccdcoe/go-peek/pkg/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	// Workers configures the number of goroutines that are spawned for running comuntational tasks
	Workers = 1
	// IgnoreParseErrors defines weather an individual message parse error should stop processing altogether
	// Alternatively, files will be read to end and total number of encountered errors will be retruned as an error type
	IgnoreParseErrors = false

	// TimeStampFormat is for parsing time range from cli arguments
	TimeStampFormat = "2006-01-02 15:04:05"
)

func Entrypoint(cmd *cobra.Command, args []string) {
	// Define in-scope variable types
	// Load config params from viper
	var (
		enableCache = viper.GetBool("cache")
		spooldir    = viper.GetString("work.dir")
	)
	Workers = viper.GetInt("work.threads")

	replayInterval, err := utils.NewIntervalFromStrings(
		viper.GetString("time.from"),
		viper.GetString("time.to"),
		TimeStampFormat,
	)
	if err != nil {
		log.Fatal(err)
	}

	if enableCache {
		log.Info("replay cache enabled")
	}

	log.WithFields(log.Fields{
		"workers": Workers,
		"dir":     spooldir,
	}).Debug("config parameter debug")

	var (
		paths         []string
		discoverFiles = make([]*Sequence, 0)
	)

	// try to see if supported event types are configured
	for _, event := range events.Atomics {
		// Early return if event type is not configured
		if paths = viper.GetStringSlice(fmt.Sprintf("stream.%s.dir", event)); len(paths) == 0 {
			log.WithFields(log.Fields{
				"type": event.String(),
			}).Trace("input not configured")
			continue
		}
		log.WithFields(log.Fields{
			"type":  event.String(),
			"src":   paths,
			"items": len(paths),
		}).Debug("configured input source")

		for _, pth := range paths {
			log.WithFields(log.Fields{
				"type": event.String(),
				"dir":  pth,
			}).Debug("configured input source")

			if pth, err = utils.ExpandHome(pth); err != nil {
				log.Fatal(err)
			}

			if !utils.StringIsValidDir(pth) {
				log.WithFields(log.Fields{
					"path": pth,
				}).Fatal("invalid path")
			}

			discoverFiles = append(discoverFiles, &Sequence{
				Type:    event,
				DataDir: pth,
			})
		}
	}

	if len(discoverFiles) == 0 {
		log.Fatal("No valid log directory paths configured")
	}

	for _, seq := range discoverFiles {
		if err := seq.Discover(); err != nil {
			log.Fatal(err)
		}

		log.WithFields(log.Fields{
			"files": len(seq.Files),
			"path":  seq.DataDir,
		}).Debug("file discovery")

		// TODO: filter out files in each sequence that do not belong to specified range here
		files := make([]*Handle, 0)
		for _, f := range seq.Files {
			if !utils.IntervalInRange(*f.Interval, *replayInterval) {
				continue
			}

			files = append(files, f.checkPartial(*replayInterval).enable())
			log.WithFields(log.Fields{
				"type":  seq.Type.String(),
				"file":  f.Base(),
				"begin": f.Interval.Beginning,
				"end":   f.Interval.End,
				"dir":   seq.DataDir,
			}).Trace("found file")

		}
		seq.Files = files
	}

	if err := SequenceList(discoverFiles).asyncBuildOrLoadAll(
		spooldir,
		enableCache,
	); err != nil {
		log.Fatal(err)
	}

	if err := SequenceList(discoverFiles).seekAll(*replayInterval); err != nil {
		log.Fatal(err)
	}

	SequenceList(discoverFiles).
		calcDiffsBetweenFiles().
		calcDiffBeginning(*replayInterval)

	if err := dumpSequences(spooldir, discoverFiles); err != nil {
		log.Fatal(err)
	}

	msgs := play(discoverFiles, *replayInterval)

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
		elaEnabled = viper.GetBool("output.elastic.enabled")
	)

	if !stdout && !fifoEnabled && !elaEnabled {
		log.Fatal("No outputs configured. See --help.")
	}

	// TODO - move code to internal/output or something, wrap for reusability in multiple subcommands
	stdoutCh := make(chan consumer.Message, 100)
	fifoCh := func() []chan consumer.Message {
		chSl := make([]chan consumer.Message, len(fifoPaths))
		for i := range fifoPaths {
			chSl[i] = make(chan consumer.Message, 100)
		}
		return chSl
	}()
	elaCh := make(chan consumer.Message, 100)

	defer close(stdoutCh)
	defer func() {
		for _, ch := range fifoCh {
			close(ch)
		}
	}()
	defer close(elaCh)

	if elaEnabled {
		var fn elastic.IdxFmtFn
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
			Workers:  2,
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
	}

	log.Debug("Successful exit")
}
