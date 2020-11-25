package run

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"go-peek/pkg/intel/assets"
	"go-peek/pkg/intel/mitre"
	"go-peek/pkg/intel/mitremeerkat"
	"go-peek/pkg/models/consumer"
	"go-peek/pkg/models/events"
	"go-peek/pkg/models/meta"
	"go-peek/pkg/parsers"
	"go-peek/pkg/utils"

	sigma "github.com/markuskont/go-sigma-rule-engine/pkg/sigma/v2"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type shipperChannels struct {
	main <-chan *consumer.Message
	emit <-chan *consumer.Message
}

func spawnWorkers(
	rx <-chan *consumer.Message,
	workers int,
	spooldir string,
	mapping consumer.ParseMap,
	assetCache *assets.Handle,
) (*shipperChannels, *utils.ErrChan) {
	tx := make(chan *consumer.Message, 0)
	var emitCh chan *consumer.Message
	if viper.GetBool("emit.elastic.enabled") ||
		viper.GetBool("emit.kafka.enabled") ||
		viper.GetBool("emit.file.enabled") {
		emitCh = make(chan *consumer.Message, 0)
		log.Info("Enabling emitter.")
	}
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
		if emitCh != nil {
			defer func() {
				log.Trace("Closing emitter")
				close(emitCh)
			}()
		}
		sourceToEvent := func(topic string) consumer.ParseMapping {
			if val, ok := mapping[topic]; ok {
				return val
			}
			return consumer.ParseMapping{}
		}
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				defer log.Tracef("worker %d done", id)
				logContext := log.WithFields(log.Fields{"id": id})
				log.Tracef("Spawning worker %d", id)

				mitreEnterpriseMapper := func() *mitre.Mapper {
					if !viper.GetBool("processor.mitre.enabled") {
						logContext.Warn("Mitre id to technique name / tactic mapper disabled!")
						return nil
					}
					logContext.Info("Reading MITRE ID and tactic mappings.")
					mitreEnterpriseMapper, err := mitre.NewMapper(mitre.Config{
						EnterpriseDump: func() string {
							if p := viper.GetString("processor.mitre.enterprise.json"); p != "" {
								expanded, err := utils.ExpandHome(p)
								if err != nil {
									logContext.Fatal(err)
								}
								logContext.Debugf("Using %s as mitre dump file", expanded)
								return expanded
							}
							dump := filepath.Join(viper.GetString("work.dir"), "mitre-enterprise.json")
							logContext.Debugf("Using %s as mitre dump file", dump)
							return dump
						}(),
					})
					if err != nil {
						logContext.Fatal(err)
					}
					logContext.Infof("Got %d MITRE technique mappings", len(mitreEnterpriseMapper.Mappings))
					return mitreEnterpriseMapper
				}()

				mitreSignatureConverter := func() *mitremeerkat.Mapper {
					if !viper.GetBool("processor.mitre.enabled") ||
						!viper.GetBool("processor.mitre.meerkat.enabled") {
						logContext.Warn("Suricata SID MITRE mapper disabled.")
						return nil
					}
					host := viper.GetString("processor.mitre.meerkat.redis.host")
					port := viper.GetInt("processor.mitre.meerkat.redis.port")
					db := viper.GetInt("processor.mitre.meerkat.redis.db")
					logContext.Infof("Suricata SID MITRE mapper connecting to %s:%d DB %d", host, port, db)
					mitreSignatureConverter, err := mitremeerkat.NewMapper(
						&mitremeerkat.Config{Host: host, Port: port, DB: db},
					)
					if err != nil {
						logContext.Fatal(err)
					}
					return mitreSignatureConverter
				}()

				ruleset, sigmaMatch := func() (*sigma.Ruleset, bool) {
					if !viper.GetBool("processor.sigma.enabled") {
						return nil, false
					}
					ruleset, err := sigma.NewRuleset(sigma.Config{
						Directory: viper.GetStringSlice("processor.sigma.dir"),
					})
					if err != nil {
						log.Fatal(err)
					}
					log.Debugf("SIGMA: Worker %d Found %d files, %d ok, %d failed, %d unsupported",
						id, ruleset.Total, ruleset.Ok, ruleset.Failed, ruleset.Unsupported)
					return ruleset, true
				}()

			loop:
				for msg := range rx {
					atomic.AddUint64(&count, 1)

					evInfo := sourceToEvent(msg.Source)
					msg.Event = evInfo.Atomic

					if noparse {
						msg.Time = time.Now()
						tx <- msg
						continue loop
					}

					ev, err := parsers.Parse(msg.Data, evInfo.Atomic, evInfo.Parser)
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
					msg.Key = evInfo.Atomic.String()

					m := e.GetAsset()
					if m == nil {
						errs.Send(fmt.Errorf(
							"unable to get m for event %s",
							string(msg.Data),
						))
						continue loop
					}

					if assetCache != nil {
						if host := m.Asset.Host; host != "" {
							if val, ok := assetCache.GetHost(host); ok {
								m.Asset = val.Copy()
							}
						} else if ip := m.Asset.IP; ip != nil {
							if val, ok := assetCache.GetIP(ip.String()); ok {
								m.Asset = val.Copy()
							}
						}
						if m.Source != nil && m.Source.IP != nil {
							ip := m.Source.IP
							if val, ok := assetCache.GetIP(ip.String()); ok {
								m.Source = val
								m.IP = ip
							}
						}
						if m.Destination != nil && m.Destination.IP != nil {
							ip := m.Destination.IP
							if val, ok := assetCache.GetIP(ip.String()); ok {
								m.Destination = val
								m.IP = ip
							}
						}
					}

					if sigmaMatch {
						if sigmaEvent, ok := ev.(sigma.Event); ok {
							if results, match := ruleset.EvalAll(sigmaEvent); match {
								m.SigmaResults = results
								// m.MitreAttack.ParseSigmaTags(results, mitreTechniqueMapper)
							}
						} else {
							errs.Send(fmt.Errorf(
								"Event type %s does not satisfy sigma.Event",
								evInfo.Atomic.String(),
							))
							continue loop
						}
					}

					// MITRE ATT&CK enrichment FROM MESSAGE
					switch obj := ev.(type) {
					case *events.Suricata:
						if mitreSignatureConverter != nil {
							if obj.Alert != nil && obj.Alert.SignatureID > 0 {
								if mapping, ok := mitreSignatureConverter.GetSid(obj.Alert.SignatureID); ok {
									m.MitreAttack.Techniques = append(m.MitreAttack.Techniques, meta.Technique{
										ID:   mapping.ID,
										Name: mapping.Name,
									})
									m.MitreAttack.Set(mitreEnterpriseMapper.Mappings)
								}
							}
						}
					case *events.DynamicWinlogbeat:
						if res := obj.MitreAttack(); res != nil {
							m.MitreAttack = res
							if mitreEnterpriseMapper != nil {
								res.Set(mitreEnterpriseMapper.Mappings)
							}
						}
					}

					if len(m.MitreAttack.Techniques) == 0 {
						m.MitreAttack = nil
					}
					if emitCh != nil {
						if m.MitreAttack != nil && m.SigmaResults != nil {
							m.EventData = e.DumpEventData()
						}
					}
					m.EventType = evInfo.Atomic.String()
					e.SetAsset(*m.SetDirection())

					modified, err := e.JSONFormat()
					if err != nil {
						errs.Send(err)
						continue loop
					}
					msg.Data = modified
					if emitCh != nil {
						if m.MitreAttack != nil || m.SigmaResults != nil {
							emitCh <- msg
						}
					}
					tx <- msg
				}
			}(i)
		}
		wg.Wait()
	}()
	return &shipperChannels{main: tx, emit: emitCh}, errs
}
