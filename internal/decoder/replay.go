package decoder

import (
	"container/list"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/ccdcoe/go-peek/internal/ingest/file"
	"github.com/ccdcoe/go-peek/internal/logging"
	"github.com/ccdcoe/go-peek/internal/types"
	"github.com/ccdcoe/go-peek/pkg/events"
)

type LogReplayWorkerConfig struct {
	From, To time.Time
	Workers  int
	Logger   logging.LogHandler
	Timeout  time.Duration
	Speedup  int64
}

type SourceStatConfig struct {
	Name   string
	Source string
	Type   string
	LogReplayWorkerConfig
}

type TimeList struct {
	Source     string
	Times      *list.List
	Start, End int64
	File       *file.LogFile
}

type TimeListSequenceMap map[string][]*TimeList

func (lm TimeListSequenceMap) Replay(config LogReplayWorkerConfig) (types.MessageChannel, error) {
	fn := func(
		src string,
		output chan types.Message,
		replaySlice []*TimeList,
		speedup int64,
		wg *sync.WaitGroup,
		logger logging.LogSender,
		srt bool,
	) {
		defer wg.Done()

		if srt {
			logger.Notify(fmt.Sprintf("Sorting files for %s", src))
			sort.Slice(replaySlice, func(i, j int) bool {
				return replaySlice[i].File.From.UnixNano() < replaySlice[j].File.From.UnixNano()
			})
		}

		fmt.Fprintf(os.Stdout, "Looping files\n")
	sourceLoop:
		for _, replay := range replaySlice {
			logger.Notify(fmt.Sprintf("%s, first offset: %d last offset: %d",
				replay.Source, replay.Start, replay.End))
			if replay.Start < 0 || replay.End < 0 {
				logger.Notify(fmt.Sprintf("%s has no known messages, skipping\n", replay.Source))
				continue sourceLoop
			}
			messages := make(chan types.Message)
			go func() {
				if replay.Start > 0 {
					logger.Notify(fmt.Sprintf("%s does not start from beginning, consuming until %d",
						replay.Source, replay.Start))
				msgLoop:
					for msg := range messages {
						if msg.Offset == replay.Start {
							break msgLoop
						}
					}
				}
				logger.Notify(fmt.Sprintf("%s consuming", replay.Source))
				for sleep := replay.Times.Front(); sleep != nil; sleep = sleep.Next() {
					msg := <-messages
					output <- msg
					time.Sleep(sleep.Value.(time.Duration) / time.Duration(speedup))
				}
			}()

			fmt.Fprintf(os.Stdout, "Reading\n")
			if err := <-replay.File.AsyncRead(messages); err != nil {
				fmt.Fprintf(os.Stderr, "Unable to read %s. %s\n", replay.Source, err.Error())
				continue sourceLoop
			}
			close(messages)
		}
	}
	output := make(types.MessageChannel)
	logger := config.Logger
	if config.Logger == nil {
		logger = logging.NewLogHandler()
	}
	go func() {
		var wg sync.WaitGroup
		for k, v := range lm {
			wg.Add(1)
			go fn(k, output, v, config.Speedup, &wg, logger, false)
		}
		wg.Wait()
		close(output)
	}()

	return output, nil
}

type EventFileInfoListing struct {
	Name string
	Type string
	Data file.FileInfoListing
}

type MultiFileInfoListing map[string]*EventFileInfoListing

func (fl MultiFileInfoListing) CollectTimeStamps(config LogReplayWorkerConfig) (TimeListSequenceMap, error) {
	timelistmap := make(map[string][]*TimeList)
	for k, v := range fl {
		if config.Logger != nil {
			config.Logger.Notify(fmt.Sprintf("Parsing timestamps from %s", k))
		}
		listsequence, err := CollectTimeStamps(v.Data, config)
		if err != nil {
			return nil, err
		}
		timelistmap[k] = listsequence
	}
	return timelistmap, nil
}
func (fl MultiFileInfoListing) EventMap() map[string]string {
	eventmap := make(map[string]string)
	for _, v := range fl {
		for _, logfile := range v.Data {
			eventmap[logfile.Path] = v.Name
		}
	}
	return eventmap
}
func (fl MultiFileInfoListing) EventTypeMap() map[string]string {
	eventmap := make(map[string]string)
	for _, v := range fl {
		for _, logfile := range v.Data {
			eventmap[logfile.Path] = v.Type
		}
	}
	return eventmap
}

// *TODO* refactor
// *TODO* This may belong in ingest/file
func CollectTimeStamps(files file.FileInfoListing, config LogReplayWorkerConfig) ([]*TimeList, error) {
	out := files.ReadFiles(config.Workers, config.Timeout)
	if config.Logger != nil {
		go func() {
			for err := range out.Logs.Errors() {
				config.Logger.Error(err)
			}
		}()
	}
	workers := 1
	if config.Workers > 1 {
		workers = config.Workers
	}

	splitter := make(map[string]chan types.Message)
	linkedListOutput := make(chan *TimeList)
	for _, v := range files {
		splitter[v.Path] = make(chan types.Message)
	}

	go func() {
		defer close(linkedListOutput)
		var wg sync.WaitGroup
		for k, v := range splitter {
			wg.Add(1)
			go func(ch chan types.Message, name string) {
				defer wg.Done()
				var first, last, count int64
				var prev time.Time

				first = -1
				last = -1

				times := list.New()
				for msg := range ch {
					if msg.Time.UnixNano() > config.From.UnixNano() && msg.Time.UnixNano() < config.To.UnixNano() {
						if first < 0 {
							first = msg.Offset
						}
						if count > 0 {
							diff := msg.Time.Sub(prev)
							times.PushBack(diff)
						}
						count++
						prev = msg.Time
						last = msg.Offset
					}
				}
				linkedListOutput <- &TimeList{
					Times:  times,
					Source: name,
					Start:  first,
					End:    last,
				}
				if config.Logger != nil {
					config.Logger.Notify(
						fmt.Sprintf("Worker done %d timestamps collected", times.Len()),
					)
				}
			}(v, k)
		}
		wg.Wait()
	}()

	safety := make(chan types.Message, 0)
	go func() {
		var wg sync.WaitGroup
		defer close(safety)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for msg := range out.Messages() {
					var TimeEvent events.SimpleTime
					if err := json.Unmarshal(msg.Data, &TimeEvent); err != nil {
						if config.Logger != nil {
							config.Logger.Error(err)
						}
					} else {
						safety <- types.Message{
							Data:   msg.Data,
							Source: msg.Source,
							Offset: msg.Offset,
							Time:   TimeEvent.GetSyslogTime(),
						}
					}
				}
			}()
		}
		wg.Wait()

	}()

	report := time.NewTicker(1 * time.Second)
	var count, total int64
loop:
	for {
		select {
		case safe, ok := <-safety:
			if !ok {
				break loop
			}
			splitter[safe.Source] <- safe
			count++
		case <-report.C:
			total = total + count
			if config.Logger != nil {
				config.Logger.Notify(
					fmt.Sprintf("total: %d, rate: %d m/sec", total, count),
				)
			}
			count = 0
		}
	}

	for _, v := range splitter {
		close(v)
	}
	replaySlice := []*TimeList{}
	for item := range linkedListOutput {
		if logfile := files.Get(item.Source); logfile != nil {
			item.File = logfile
			replaySlice = append(replaySlice, item)
		}
	}
	if len(replaySlice) == 0 {
		return replaySlice, fmt.Errorf("No time lists received for")
	}
	sort.Slice(replaySlice, func(i, j int) bool {
		return replaySlice[i].File.From.UnixNano() < replaySlice[j].File.From.UnixNano()
	})
	return replaySlice, nil
}

func MultiListLogFilesAndStatEventStart(config []SourceStatConfig) (MultiFileInfoListing, error) {
	out := make(map[string]*EventFileInfoListing)
	for i, conf := range config {
		if conf.Name == "" {
			return out, fmt.Errorf("Log event type name missing for replay element %d", i)
		}
		if conf.Source == "" {
			return out, fmt.Errorf("Log replay source missing for %s", conf.Name)
		}
		loglist, err := ListLogFilesAndStatEventStart(conf)
		if err != nil {
			return out, nil
		}
		out[conf.Source] = &EventFileInfoListing{
			Data: loglist,
			Name: conf.Name,
			Type: conf.Type,
		}
	}
	return out, nil
}

func ListLogFilesAndStatEventStart(config SourceStatConfig) (file.FileInfoListing, error) {
	fileGen, err := file.ListFilesGenerator(config.Source, nil)
	if err != nil {
		return nil, err
	}
	if config.Logger != nil {
		config.Logger.Notify(fmt.Sprintf("slicing files for %s", config.Source))
	}
	files := fileGen.Slice()
	if err := <-files.StartTimes(config.Workers, config.Timeout, func(
		line []byte,
		logfile *file.LogFile,
	) (time.Time, error) {
		var TimeEvent events.SimpleTime
		if err := json.Unmarshal(line, &TimeEvent); err != nil {
			return time.Now(), err
		}
		tme := TimeEvent.GetSyslogTime()
		return tme, nil
	}); err != nil {
		return nil, err
	}
	if config.Logger != nil {
		config.Logger.Notify(fmt.Sprintf("sorting files in %s", config.Source))
	}
	files = files.SortByTime().Prune(config.From, config.To, true)
	if err := <-files.StatFiles(config.Workers, config.Timeout); err != nil {
		return files, err
	}
	return files, nil
}
