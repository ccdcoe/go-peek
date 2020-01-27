package run

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ccdcoe/go-peek/internal/engines/shipper"
	"github.com/ccdcoe/go-peek/pkg/intel/assetcache"
	"github.com/ccdcoe/go-peek/pkg/intel/mitremeerkat"
	"github.com/ccdcoe/go-peek/pkg/intel/wise"
	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	"github.com/ccdcoe/go-peek/pkg/models/events"
	"github.com/ccdcoe/go-peek/pkg/models/meta"
	"github.com/ccdcoe/go-peek/pkg/parsers"
	"github.com/ccdcoe/go-peek/pkg/utils"
	"github.com/markuskont/go-sigma-rule-engine/pkg/sigma"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func spawnWorkers(
	rx <-chan *consumer.Message,
	workers int,
	spooldir string,
	mapping consumer.ParseMap,
) (<-chan *consumer.Message, *utils.ErrChan) {
	tx := make(chan *consumer.Message, 0)
	errs := utils.NewErrChan(100, "Event parse worker runtime errors")
	var wg sync.WaitGroup
	noparse := func() bool {
		if !viper.GetBool("processor.enabled") {
			log.Debug("all procesor plugins disabled globally")
			return true
		}
		return false
	}()
	var (
		every = time.NewTicker(3 * time.Second)
		count uint64
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

	emitCh := make(chan *consumer.Message, 100)
	emit := true
	go func() {
		defer close(emitCh)
		if err := shipper.Send(emitCh, "emit"); err != nil {
			switch err.(type) {
			case shipper.ErrNoOutputs, *shipper.ErrNoOutputs:
				emit = false
				log.Warn("Emitter has no outputs. Will not be enabled.")
			default:
				log.Fatal(err)
			}
		}
		emitCh = nil
	}()

	go func(emit bool) {
		defer close(tx)
		defer close(errs.Items)
		sourceToEvent := func(topic string) consumer.ParseMapping {
			if val, ok := mapping[topic]; ok {
				return val
			}
			return consumer.ParseMapping{}
		}
		defer globalAssetCache.Close()
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(id int, emit bool) {
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
				mitreTechniqueMapper := func() meta.Techniques {
					if path := viper.GetString("processor.mitre.technique.json"); path != "" {
						out, err := meta.NewTechniquesFromJSONfile(path)
						if err != nil {
							log.Fatal(err)
						}
						log.Infof("worker %d loaded %d mappings from %s", id, len(out), path)
						return out
					}
					return nil
				}()

				ruleset, checkRules, quickmatch := func() (*sigma.Ruleset, bool, bool) {
					if !viper.GetBool("processor.sigma.enabled") {
						return nil, false, false
					}
					ruleset, err := sigma.NewRuleset(&sigma.Config{
						Directories: viper.GetStringSlice("processor.sigma.dir"),
					})
					if err != nil {
						log.Fatal(err)
					}
					for _, unsupp := range ruleset.Unsupported {
						log.Warnf("%+v", unsupp)
					}
					for _, err := range ruleset.Broken {
						log.Errorf("%+v", err)
					}
					log.Infof(
						"Successfully parsed %d sigma rules. Unsupported %d. Broken %d",
						ruleset.Total,
						len(ruleset.Unsupported),
						len(ruleset.Broken),
					)
					return ruleset, true, viper.GetBool("processor.sigma.quickmatch")
				}()
			loop:
				for msg := range rx {
					atomic.AddUint64(&count, 1)

					evInfo := sourceToEvent(msg.Source)
					evType := evInfo.Atomic
					evParse := evInfo.Parser
					msg.Event = evType

					if noparse {
						msg.Time = time.Now()
						tx <- msg
						continue loop
					}

					ev, err := parsers.Parse(msg.Data, evType, evParse)
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
						if val, ok := localAssetCache.GetIP(ip.String()); ok && val.IsAsset {
							m.Asset = *val.Data
						}
					} else if host := m.Asset.Host; host != "" {
						if val, ok := localAssetCache.GetString(host); ok && val.IsAsset {
							m.Asset = *val.Data
						}
					}
					if m.Source != nil {
						if ip := m.Source.IP; ip != nil {
							if val, ok := localAssetCache.GetIP(ip.String()); ok && val.IsAsset && val.Data != nil {
								m.Source = val.Data
								m.Source.IP = ip
							}
						}
					}
					if m.Destination != nil {
						if ip := m.Destination.IP; ip != nil {
							if val, ok := localAssetCache.GetIP(ip.String()); ok && val.IsAsset && val.Data != nil {
								m.Destination = val.Data
								m.Destination.IP = ip
							}
						}
					}

					if checkRules {
						if obj, ok := ev.(sigma.EventChecker); ok {
							if res, match := ruleset.Rules.Check(obj, evType.String(), quickmatch); match {
								m.SigmaResults = res
							}
						}
						if m.SigmaResults != nil {
							m.MitreAttack.ParseSigmaTags(m.SigmaResults, mitreTechniqueMapper)
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
								m.MitreAttack.Set(mitreTechniqueMapper)
							}
						}
					case *events.DynamicWinlogbeat:
						if res := obj.MitreAttack(); res != nil {
							res.Set(mitreTechniqueMapper)
							m.MitreAttack = res
						}
					}

					if len(m.MitreAttack.Techniques) == 0 {
						m.MitreAttack = nil
					}
					if emitCh != nil && (m.MitreAttack != nil || m.SigmaResults != nil) {
						m.EventData = e.DumpEventData()
					}
					m.EventType = evType.String()
					e.SetAsset(*m.SetDirection())

					modified, err := e.JSONFormat()
					if err != nil {
						errs.Send(err)
						continue loop
					}
					msg.Data = modified
					if emitCh != nil && (m.MitreAttack != nil || m.SigmaResults != nil) {
						emitCh <- msg
					}
					tx <- msg
				}
			}(i, emit)
		}
		wg.Wait()
	}(emit)
	return tx, errs
}
