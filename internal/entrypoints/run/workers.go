package run

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/ccdcoe/go-peek/pkg/intel"
	"github.com/ccdcoe/go-peek/pkg/intel/wise"
	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	"github.com/ccdcoe/go-peek/pkg/models/events"
	"github.com/ccdcoe/go-peek/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func spawnWorkers(
	rx <-chan *consumer.Message,
	workers int,
	spooldir string,
) (<-chan *consumer.Message, *utils.ErrChan) {
	tx := make(chan *consumer.Message, 0)
	kafkaTopicToEvent := func(topic string) events.Atomic {
		mapping := func() map[string]events.Atomic {
			out := make(map[string]events.Atomic)
			for _, event := range events.Atomics {
				if src := viper.GetStringSlice(
					fmt.Sprintf("stream.%s.kafka.topic", event.String()),
				); len(src) > 0 {
					for _, item := range src {
						out[item] = event
					}
				}
			}
			return out
		}()
		if val, ok := mapping[topic]; ok {
			return val
		}
		return events.SimpleE
	}
	errs := utils.NewErrChan(100, "Event parse worker runtime errors")
	var wg sync.WaitGroup
	anon := viper.GetBool("processor.anonymize")
	noparse := func() bool {
		if !viper.GetBool("processor.enabled") {
			log.Debug("all procesor plugins disabled globally, only parsing for timestamps")
			return true
		}
		return false
	}()
	globalAssetCache, err := intel.NewGlobalCache(&intel.Config{
		Wise: func() *wise.Config {
			if viper.GetBool("processor.inputs.wise.enabled") {
				wh := viper.GetString("processor.inputs.wise.host")
				log.Debugf("wise enabled, configuring for host %s", wh)
				return &wise.Config{Host: wh}
			}
			return nil
		}(),
		Prune: true,
		DumpJSONAssets: func() string {
			pth := viper.GetString("processor.persist.json.assets")
			if exp, err := utils.ExpandHome(pth); err == nil {
				pth = exp
			}
			if pth == "" || filepath.IsAbs(pth) {
				return pth
			}
			return filepath.Join(spooldir, filepath.Base(pth))
		}(),
		LoadJSONnets: func() string {
			pth := viper.GetString("processor.persist.json.networks")
			if exp, err := utils.ExpandHome(pth); err == nil {
				pth = exp
			}
			if pth == "" || filepath.IsAbs(pth) {
				return pth
			}
			return filepath.Join(spooldir, filepath.Base(pth))
		}(),
	})
	logContext := log.WithFields(log.Fields{
		"action": "init global cache",
		"thread": "main spawn",
	})
	if err != nil && !noparse {
		logContext.Fatal(err)
	} else if err != nil && noparse {
		logContext.Warn(err)
	}
	go func() {
		defer close(tx)
		defer close(errs.Items)
		defer globalAssetCache.Close()
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				defer log.Tracef("worker %d done", id)

				log.Tracef("Spawning worker %d", id)
				localAssetCache := intel.NewLocalCache(globalAssetCache, id)
				defer localAssetCache.Close()

			loop:
				for msg := range rx {
					evType := msg.Event
					switch msg.Type {
					case consumer.Kafka:
						evType = kafkaTopicToEvent(msg.Source)
					}
					if noparse {
						msg.Time = time.Now()
						tx <- msg
						continue loop
					}

					e, err := events.NewGameEvent(msg.Data, evType)
					if err != nil {
						errs.Send(err)
						continue loop
					}
					msg.Time = e.Time()

					meta := e.GetAsset()
					if meta == nil {
						errs.Send(fmt.Errorf(
							"unable to get meta for event %s",
							string(msg.Data),
						))
						continue loop
					}

					// Asset checking stuff
					if ip := meta.Asset.IP; ip != nil {
						if val, ok := localAssetCache.GetIP(ip); ok && val.IsAsset {
							meta.Asset = *val.Data
						}
					}
					if meta.Source != nil {
						if ip := meta.Source.IP; ip != nil {
							if val, ok := localAssetCache.GetIP(ip); ok && val.IsAsset && val.Data != nil {
								meta.Source = val.Data
							}
						}
					}
					if meta.Destination != nil {
						if ip := meta.Destination.IP; ip != nil {
							if val, ok := localAssetCache.GetIP(ip); ok && val.IsAsset && val.Data != nil {
								meta.Destination = val.Data
							}
						}
					}

					meta.SetDirection()
					if anon {
						// TODO - also get rid of host name fields in message
						meta.Host = meta.Alias
						if meta.Source != nil {
							meta.Source.Host = meta.Source.Alias
						}
						if meta.Destination != nil {
							meta.Destination.Host = meta.Destination.Alias
						}
					}

					e.SetAsset(*meta)
					modified, err := e.JSONFormat()
					if err != nil {
						errs.Send(err)
						continue loop
					}
					msg.Data = modified
					tx <- msg
				}
			}(i)
		}
		wg.Wait()
	}()
	return tx, errs
}
