package replay

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/ccdcoe/go-peek/internal/ingest/v2"
	"github.com/ccdcoe/go-peek/pkg/events/v2"
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
		pth           string
		discoverFiles = make([]*Sequence, 0)
	)

	// try to see if supported event types are configured
	for _, event := range events.Atomics {
		// Early return if event type is not configured
		if pth = viper.GetString(fmt.Sprintf("stream.%s.dir", event)); pth == "" {
			log.WithFields(log.Fields{
				"type": event.String(),
			}).Trace("input not configured")
			continue
		}
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

	SequenceList(discoverFiles).calcDiffsBetweenFiles().calcDiffBeginning(*replayInterval)

	if err := dumpSequences(spooldir, discoverFiles); err != nil {
		log.Fatal(err)
	}

	var (
		stdout   = viper.GetBool("play.stdout.enable")
		fifo     = viper.GetBool("play.fifo.enable")
		fifoPath = viper.GetString("play.fifo.path")
	)

	if !stdout && !fifo {
		log.Fatal("No outputs configured. See --help.")
	}

	msgs := play(discoverFiles, *replayInterval)
	stdoutCh := make(chan *ingest.Message, 3)
	fifoCh := make(chan *ingest.Message, 3)

	defer close(stdoutCh)
	defer close(fifoCh)

	if stdout {
		log.Info("stdout enabled, starting handler")
		go func(rx <-chan *ingest.Message) {
			for msg := range rx {
				fmt.Fprintf(os.Stdout, "%s\n", string(msg.Data))
			}
		}(stdoutCh)
	}

	if fifo {
		log.Infof("fifo enabled, starting handler for %s", fifoPath)
		pipe, err := os.OpenFile(fifoPath, os.O_RDWR, os.ModeNamedPipe)
		if err != nil {
			log.Fatal(err)
		}
		defer pipe.Close()
		go func(rx <-chan *ingest.Message) {
			for msg := range rx {
				fmt.Fprintf(pipe, "%s\n", string(msg.Data))
			}
		}(fifoCh)
	}

	for m := range msgs {
		if stdout {
			stdoutCh <- m
		}
		if fifo {
			fifoCh <- m
		}
	}

	log.Debug("Successful exit")
}
