package enrich

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"go-peek/pkg/intel/mitre"
	"go-peek/pkg/mitremeerkat"
	"go-peek/pkg/models/atomic"
	"go-peek/pkg/models/events"
	"go-peek/pkg/models/meta"
	"go-peek/pkg/persist"
	"go-peek/pkg/providentia"

	"github.com/dgraph-io/badger/v3"
	jsoniter "github.com/json-iterator/go"
	"github.com/markuskont/go-sigma-rule-engine/pkg/sigma/v2"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	badgerPrefix        = "assets"
	badgerSidMapKey     = "enrich-suricata-sid-map"
	badgerMissingSidKey = "enrich-suricata-sid-missing"
)

var ErrMissingPersist = errors.New("missing badgerdb persistance")

type ErrMissingAssetData struct{ Event events.GameEvent }

func (e ErrMissingAssetData) Error() string {
	return fmt.Sprintf("missing asset data for %+v", e.Event)
}

type SigmaConfig struct {
	Kind events.Atomic
	Path string
}

type Config struct {
	Persist *persist.Badger
	Mitre   mitre.Config
	Sigma   map[events.Atomic]sigma.Ruleset
}

func (c Config) Validate() error {
	if c.Persist == nil {
		return ErrMissingPersist
	}
	return nil
}

type Counts struct {
	Events      uint
	MissingMeta uint

	AssetPickups uint
	AssetUpdates uint
	Assets       int

	MappedMitreSIDs int

	ParseErrs  countsParseErrs
	Enrichment lookups
	Problems   problems
}

type countsParseErrs struct {
	Suricata uint
	Windows  uint
	Syslog   uint
	Snoopy   uint
}

type lookups struct {
	SuricataSidMatches uint
	SuricataSidMisses  uint

	SigmaMatches   uint
	SigmaMisses    uint
	SigmaNoRuleset uint
}

type problems struct {
	MissingSuricataTimestamp uint
}

type Handler struct {
	Counts

	missingLookupSet map[string]bool
	missingSidMaps   map[int]string

	sigma map[events.Atomic]sigma.Ruleset

	// mitre tag per suricata SID
	sidTag map[int]string

	mitre   *mitre.Mapper
	assets  map[string]providentia.Record
	persist *persist.Badger
}

func (h Handler) MissingSidMaps() map[int]string {
	if h.missingSidMaps == nil {
		return map[int]string{}
	}
	return h.missingSidMaps
}

func (h Handler) MissingKeys() []string {
	keys := make([]string, 0, len(h.missingLookupSet))
	for key := range h.missingLookupSet {
		keys = append(keys, key)
	}
	return keys
}

func (h *Handler) AddSidTag(item mitremeerkat.Mapping) *Handler {
	if h.sidTag == nil {
		h.sidTag = make(map[int]string)
	}
	h.sidTag[item.SID] = item.ID
	_, ok := h.missingSidMaps[item.SID]
	if ok {
		delete(h.missingSidMaps, item.SID)
	}
	h.MappedMitreSIDs = len(h.sidTag)
	return h
}

func (h Handler) Persist() error {
	if err := h.persist.SetSingle(badgerSidMapKey, h.sidTag); err != nil {
		return err
	}
	if err := h.persist.SetSingle(badgerMissingSidKey, h.missingSidMaps); err != nil {
		return err
	}
	return nil
}

func (h *Handler) AddAsset(value providentia.Record) *Handler {
	h.AssetPickups++
	for _, key := range value.Keys() {
		_, ok := h.assets[key]
		if !ok {
			h.AssetUpdates++
			h.persist.Set(badgerPrefix, persist.GenericValue{Key: key, Data: value})
			h.assets[key] = value
		}
	}
	h.Assets = len(h.assets)
	return h
}

func (h *Handler) Decode(raw []byte, kind events.Atomic) (events.GameEvent, error) {
	var event events.GameEvent
	h.Events++

	switch kind {
	case events.SuricataE:
		var obj atomic.StaticSuricataEve
		if err := json.Unmarshal(raw, &obj); err != nil {
			h.ParseErrs.Suricata++
			return nil, err
		}
		if ts := obj.Time(); ts.IsZero() {
			h.Problems.MissingSuricataTimestamp++
		}
		event = &events.Suricata{
			Timestamp:         obj.Time(),
			StaticSuricataEve: obj,
		}
	case events.EventLogE, events.SysmonE:
		var obj atomic.DynamicWinlogbeat
		if err := json.Unmarshal(raw, &obj); err != nil {
			h.ParseErrs.Windows++
			return nil, err
		}
		event = &events.DynamicWinlogbeat{
			Timestamp:         obj.Time(),
			DynamicWinlogbeat: obj,
		}
	case events.SyslogE:
		var obj events.Syslog
		if err := json.Unmarshal(raw, &obj); err != nil {
			h.ParseErrs.Syslog++
			return nil, err
		}
		event = &obj
	case events.SnoopyE:
		var obj events.Snoopy
		if err := json.Unmarshal(raw, &obj); err != nil {
			h.ParseErrs.Snoopy++
			return nil, err
		}
		event = &obj
	}
	return event, nil
}

func (h *Handler) Enrich(event events.GameEvent) error {
	// get blank asset template with info from message
	fullAsset := event.GetAsset()
	if fullAsset == nil {
		return ErrMissingAssetData{event}
	}

	// do asset db lookup
	fullAsset.Asset = *h.assetLookup(fullAsset.Asset)
	if fullAsset.Source != nil {
		fullAsset.Source = h.assetLookup(*fullAsset.Source)
	}
	if fullAsset.Destination != nil {
		fullAsset.Destination = h.assetLookup(*fullAsset.Destination)
	}

	// SIGMA match
	if h.sigma != nil {
		ruleset, ok := h.sigma[event.Kind()]
		if ok {
			if result, match := ruleset.EvalAll(event); match && len(result) > 0 {
				fullAsset.SigmaResults = result
				fullAsset.MitreAttack.ParseSigmaTags(fullAsset.SigmaResults, h.mitre.Mappings)
				h.Enrichment.SigmaMatches++
			} else {
				h.Enrichment.SigmaMisses++
			}
		} else {
			h.Enrichment.SigmaNoRuleset++
		}
	}

	// add MITRE ATT&CK info
	if mitreInfo := event.GetMitreAttack(); mitreInfo != nil {
		mitreInfo.Set(h.mitre.Mappings)
		fullAsset.MitreAttack = mitreInfo
	}

	switch event.Kind() {
	case events.SuricataE:
		// need alert SID, doubt theres any other way than typecasting...
		strict, ok := event.(*events.Suricata)
		if ok && h.sidTag != nil && strict.Alert != nil {
			if val, present := h.sidTag[strict.Alert.SignatureID]; present {
				fullAsset.MitreAttack.Techniques = append(
					fullAsset.MitreAttack.Techniques,
					meta.Technique{ID: val},
				)
				fullAsset.MitreAttack.Set(h.mitre.Mappings)
				h.Enrichment.SuricataSidMatches++
			} else {
				h.Enrichment.SuricataSidMisses++
				h.missingSidMaps[strict.Alert.SignatureID] = strict.Alert.Signature
			}
		}
	}

	fullAsset.EventData = event.DumpEventData()
	fullAsset.EventType = event.Kind().String()

	// object is initialized with empty techniques, set nil if still empty for later emit check
	if len(fullAsset.MitreAttack.Techniques) == 0 {
		fullAsset.MitreAttack = nil
	}
	event.SetAsset(fullAsset.SetDirection())

	return nil
}

func (h Handler) assetLookup(asset meta.Asset) *meta.Asset {
	if asset.IP != nil {
		if val, ok := h.assets[asset.IP.String()]; ok {
			return val.Asset()
		}
	}
	if asset.Host != "" {
		if val, ok := h.assets[asset.Host]; ok {
			delete(h.missingLookupSet, asset.Host)
			return val.Asset()
		} else if fqdn := asset.FQDN(); fqdn != "" {
			if val, ok := h.assets[fqdn]; ok {
				delete(h.missingLookupSet, fqdn)
				return val.Asset()
			}
		}
		h.missingLookupSet[asset.Host] = true
	}
	return &asset
}

func (h Handler) Close() error {
	return nil
}

func NewHandler(c Config) (*Handler, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	handler := &Handler{
		persist:          c.Persist,
		missingLookupSet: make(map[string]bool),
	}
	assets := make(map[string]providentia.Record)
	records := c.Persist.Scan(badgerPrefix)

	for record := range records {
		var obj providentia.Record
		buf := bytes.NewBuffer(record.Data)
		err := gob.NewDecoder(buf).Decode(&obj)
		if err != nil {
			return nil, err
		}
		for _, key := range obj.Keys() {
			assets[key] = obj
		}
	}
	handler.assets = assets
	handler.Assets = len(handler.assets)

	m, err := mitre.NewMapper(c.Mitre)
	if err != nil {
		return nil, err
	}
	handler.mitre = m

	if c.Sigma != nil && len(c.Sigma) > 0 {
		handler.sigma = c.Sigma
	}

	sidTag, err := getSidMap(badgerSidMapKey, handler.persist)
	if err != nil {
		return nil, err
	}
	handler.sidTag = sidTag
	handler.MappedMitreSIDs = len(sidTag)

	missingSidTag, err := getSidMap(badgerMissingSidKey, handler.persist)
	if err != nil {
		return nil, err
	}
	handler.missingSidMaps = missingSidTag

	return handler, nil
}

func getSidMap(key string, p *persist.Badger) (map[int]string, error) {
	var data map[int]string
	err := p.GetSingle(key, func(b []byte) error {
		buf := bytes.NewBuffer(b)
		return gob.NewDecoder(buf).Decode(&data)
	})
	if err != nil && err != badger.ErrKeyNotFound {
		return data, err
	}
	if data == nil {
		data = make(map[int]string)
	}
	return data, nil
}
