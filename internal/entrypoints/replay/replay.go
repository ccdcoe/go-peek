package replay

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/ccdcoe/go-peek/internal/engines/directory"
	"github.com/ccdcoe/go-peek/internal/engines/shipper"
	"github.com/ccdcoe/go-peek/pkg/models/events"
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
	directory.TimeStampFormat = TimeStampFormat
	directory.Fn = getIntervalFromJSON
	directory.Workers = Workers

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
		discoverFiles = make([]*directory.Sequence, 0)
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

			discoverFiles = append(discoverFiles, &directory.Sequence{
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
		files := make([]*directory.Handle, 0)
		for _, f := range seq.Files {
			if !utils.IntervalInRange(*f.Interval, *replayInterval) {
				continue
			}

			files = append(files, f.CheckPartial(*replayInterval).Enable())
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

	if err := directory.SequenceList(discoverFiles).AsyncBuildOrLoadAll(
		spooldir,
		enableCache,
	); err != nil {
		log.Fatal(err)
	}

	if err := directory.SequenceList(discoverFiles).SeekAll(*replayInterval); err != nil {
		log.Fatal(err)
	}

	directory.SequenceList(discoverFiles).
		CalcDiffsBetweenFiles().
		CalcDiffBeginning(*replayInterval)

	if err := directory.DumpSequences(spooldir, discoverFiles); err != nil {
		log.Fatal(err)
	}

	if err := shipper.Send(play(discoverFiles, *replayInterval), "output"); err != nil {
		log.Fatal(err)
	}

	log.Debug("Successful exit")
}
