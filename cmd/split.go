package cmd

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/ccdcoe/go-peek/internal/engines/directory"
	"github.com/ccdcoe/go-peek/pkg/ingest/logfile"
	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	events "github.com/ccdcoe/go-peek/pkg/models/events"
	"github.com/ccdcoe/go-peek/pkg/timebin"
	"github.com/ccdcoe/go-peek/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// splitCmd represents the split command
var splitCmd = &cobra.Command{
	Use:   "split",
	Short: "Split input log files into a bucketed list of output files",
	Long: `Reads a set of input log files and outputs messages into a sequence of output log files while also allowing user to specify desired interval and new duration per output file. In other words, existing log files can be split with more fine-grained parameter. 
	
	For example, selection of hourly rotated logs can be split into four parts where each part holds an interval of 15 minutes, daily rotated logs can be split into hourly, etc.`,
	Run: doSplit,
}

func doSplit(cmd *cobra.Command, args []string) {
	var (
		spooldir = viper.GetString("work.dir")
		//workers  = viper.GetInt("work.threads")
		pattern = viper.GetString("input.name.pattern")
	)

	var (
		pth           string
		err           error
		cPattern      *regexp.Regexp
		discoverFiles = make([]*directory.Sequence, 0)
	)
	if cPattern, err = regexp.Compile(pattern); err != nil {
		log.Fatal(err)
	}

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

		s := &directory.Sequence{
			Type:    event,
			DataDir: pth,
		}
		if err := s.Discover(); err != nil {
			log.Fatal(err)
		}

		handles := make([]*directory.Handle, 0)
		for _, h := range s.Files {
			if cPattern.MatchString(h.Base()) {
				log.WithFields(log.Fields{
					"file":    h.Path.String(),
					"pattern": pattern,
				}).Debug("regex match")
				handles = append(handles, h)
			}
		}

		s.Files = handles
		discoverFiles = append(discoverFiles, s)
	}

	if len(discoverFiles) == 0 {
		log.Fatal("No valid log directory paths configured")
	}

	stream := make(chan *consumer.Message, 0)
	go func(stream chan *consumer.Message) {
		var wg sync.WaitGroup

		for _, seq := range discoverFiles {
			wg.Add(1)
			go func(s directory.Sequence, tx chan<- *consumer.Message, wg *sync.WaitGroup) {
				defer wg.Done()

				for _, h := range s.Files {

					var obj events.KnownTimeStamps
					msgs := logfile.Drain(*h.Handle, context.Background())
					id := s.ID()

					for msg := range msgs {
						if err := json.Unmarshal(msg.Data, &obj); err == nil {
							msg.Time = obj.Time()
							msg.Source = id
							tx <- msg
						}
					}
				}

			}(*seq, stream, &wg)
		}

		wg.Wait()
		close(stream)
	}(stream)

	// TODO: move to function or method
	intr := &utils.Interval{}
	i := 0
	for _, seq := range discoverFiles {
		for _, h := range seq.Files {
			if i == 0 || h.Interval.Beginning.Before(intr.Beginning) {
				intr.Beginning = h.Interval.Beginning
			}
			if i == 0 || h.Interval.End.After(intr.End) {
				intr.End = h.Interval.End
			}
			i++
		}
	}

	bins, err := timebin.New(*intr, viper.GetDuration("output.window.duration"))
	if err != nil {
		log.Fatal(err)
	}

	files := make(map[string][]io.WriteCloser)
	for _, seq := range discoverFiles {
		files[seq.ID()] = make([]io.WriteCloser, bins.Count)
	}

	for k, v := range files {
		for i := range v {
			out := filepath.Join(spooldir, "split", k+"-"+strconv.Itoa(i)+".gz")
			if out, err = utils.ExpandHome(out); err != nil {
				log.Fatal(err)
			}

			f, err := os.Create(out)
			if err != nil {
				log.Fatal(err)
			}
			files[k][i] = gzip.NewWriter(f)
		}
	}

	for msg := range stream {
		idx := bins.Locate(msg.Time)
		if idx > 0 || idx < len(files)-1 {
			fmt.Fprintf(files[msg.Source][idx], "%s\n", string(msg.Data))
		} else {
			fmt.Println(idx, len(files), msg.Time.String())
		}
	}

	for _, v := range files {
		for _, w := range v {
			w.Close()
		}
	}
	log.Info("split function exited successfully")
}

func init() {
	rootCmd.AddCommand(splitCmd)

	// We need to know first and last timestamps but host clocks may be wrong
	// Configure parser to always prefer the syslog timestamp
	events.AlwaysUseSyslogTimestamp = true

	//replay.IgnoreParseErrors = true
	//replay.TimeStampFormat = argTsFormat

	splitCmd.Flags().String(
		"input-name-pattern", `^.+$`, `Filename regex pattern for input log files`)
	viper.BindPFlag("input.name.pattern", splitCmd.Flags().Lookup("input-name-pattern"))

	splitCmd.Flags().Duration(
		"output-window-duration", 1*time.Hour, `Output file duration.`)
	viper.BindPFlag("output.window.duration", splitCmd.Flags().Lookup("output-window-duration"))
}
