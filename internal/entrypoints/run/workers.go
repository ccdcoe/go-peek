package run

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ccdcoe/go-peek/pkg/intel/assetcache"
	"github.com/ccdcoe/go-peek/pkg/intel/mitremeerkat"
	"github.com/ccdcoe/go-peek/pkg/intel/wise"
	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	"github.com/ccdcoe/go-peek/pkg/models/events"
	"github.com/ccdcoe/go-peek/pkg/models/meta"
	"github.com/ccdcoe/go-peek/pkg/parsers"
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
	errs := utils.NewErrChan(100, "Event parse worker runtime errors")
	var wg sync.WaitGroup
	noparse := func() bool {
		if !viper.GetBool("processor.enabled") {
			log.Debug("all procesor plugins disabled globally, only parsing for timestamps")
			return true
		}
		return false
	}()
	var (
		count uint64
		every = time.NewTicker(3 * time.Second)
	)
	globalAssetCache, err := assetcache.NewGlobalCache(&assetcache.Config{
		Wise: func() *wise.Config {
			if viper.GetBool("processor.inputs.wise.enabled") {
				wh := viper.GetString("processor.inputs.wise.host")
				log.Debugf("wise enabled, configuring for host %s", wh)
				return &wise.Config{Host: wh}
			}
			return nil
		}(),
		Prune: true,
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
		for {
			select {
			case <-every.C:
				log.Infof("Collected %d events", atomic.LoadUint64(&count))
			}
		}
	}()
	go func() {
		defer close(tx)
		defer close(errs.Items)
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
		kafkaTopicToEvent := func(topic string) events.Atomic {
			if val, ok := mapping[topic]; ok {
				return val
			}
			return events.SimpleE
		}
		defer globalAssetCache.Close()
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				defer log.Tracef("worker %d done", id)
				logContext := log.WithFields(log.Fields{
					"id": id,
				})

				log.Tracef("Spawning worker %d", id)
				localAssetCache := assetcache.NewLocalCache(globalAssetCache, id)
				defer localAssetCache.Close()

				mitreSignatureConverter, err := mitremeerkat.NewMapper(&mitremeerkat.Config{
					Host: viper.GetString("processor.inputs.redis.host"),
					Port: viper.GetInt("processor.inputs.redis.port"),
					DB:   viper.GetInt("processor.inputs.redis.db"),
				})
				if err != nil {
					logContext.Fatal(err)
				}

			loop:
				for msg := range rx {
					atomic.AddUint64(&count, 1)
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

					ev, err := parsers.ParseSyslogGameEvent(msg.Data, evType)
					if err != nil {
						errs.Send(err)
						continue loop
					}
					e, ok := ev.(events.GameEvent)
					if !ok {
						errs.Send(fmt.Errorf("invalid game event type cast"))
						continue loop
					}
					msg.Time = e.Time()
					msg.Key = evType.String()

					m := e.GetAsset()
					if m == nil {
						errs.Send(fmt.Errorf(
							"unable to get m for event %s",
							string(msg.Data),
						))
						continue loop
					}

					// Asset checking stuff
					if ip := m.Asset.IP; ip != nil {
						if val, ok := localAssetCache.GetIP(ip); ok && val.IsAsset {
							m.Asset = *val.Data
						}
					} else if host := m.Asset.Host; host != "" {
						if val, ok := localAssetCache.GetString(host); ok && val.IsAsset {
							m.Asset = *val.Data
						}
					}
					if m.Source != nil {
						if ip := m.Source.IP; ip != nil {
							if val, ok := localAssetCache.GetIP(ip); ok && val.IsAsset && val.Data != nil {
								m.Source = val.Data
								m.Source.IP = ip
							}
						}
					}
					if m.Destination != nil {
						if ip := m.Destination.IP; ip != nil {
							if val, ok := localAssetCache.GetIP(ip); ok && val.IsAsset && val.Data != nil {
								m.Destination = val.Data
								m.Destination.IP = ip
							}
						}
					}
					switch obj := ev.(type) {
					case *events.Suricata:
						if obj.Alert != nil && obj.Alert.SignatureID > 0 {
							if mapping, ok := mitreSignatureConverter.GetSid(obj.Alert.SignatureID); ok {
								m.MitreAttack.Techniques = append(m.MitreAttack.Techniques, meta.Technique{
									ID:   mapping.ID,
									Name: mapping.Name,
								})
							}
						}
					}

					e.SetAsset(*m.SetDirection())
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
