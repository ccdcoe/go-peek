package enrich

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"go-peek/pkg/intel/mitre"
	"go-peek/pkg/models/events"
	"go-peek/pkg/models/meta"
	"go-peek/pkg/persist"
	"go-peek/pkg/providentia"

	"github.com/markuskont/go-sigma-rule-engine/pkg/sigma/v2"
)

const badgerPrefix = "assets"

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

	ParseErrs countsParseErrs
}

type countsParseErrs struct {
	Suricata uint
	Windows  uint
	Syslog   uint
	Snoopy   uint
}

type Handler struct {
	Counts

	missingLookupSet map[string]bool

	sigma map[events.Atomic]sigma.Ruleset

	mitre   *mitre.Mapper
	assets  map[string]providentia.Record
	persist *persist.Badger
}

func (h Handler) MissingKeys() []string {
	keys := make([]string, 0, len(h.missingLookupSet))
	for key := range h.missingLookupSet {
		keys = append(keys, key)
	}
	return keys
}

func (h *Handler) AddAsset(value providentia.Record) *Handler {
	h.AssetPickups++
	for _, key := range value.Keys() {
		_, ok := h.assets[key]
		if !ok {
			h.Counts.AssetUpdates++
			h.persist.Set(badgerPrefix, persist.GenericValue{Key: key, Data: value})
			h.assets[key] = value
		}
	}
	h.Counts.Assets = len(h.assets)
	return h
}

func (h *Handler) Decode(raw []byte, kind events.Atomic) (events.GameEvent, error) {
	var event events.GameEvent
	h.Counts.Events++

	switch kind {
	case events.SuricataE:
		var obj events.Suricata
		if err := json.Unmarshal(raw, &obj); err != nil {
			h.Counts.ParseErrs.Suricata++
			return nil, err
		}
		event = &obj
	case events.EventLogE, events.SysmonE:
		var obj events.DynamicWinlogbeat
		if err := json.Unmarshal(raw, &obj); err != nil {
			h.Counts.ParseErrs.Windows++
			return nil, err
		}
		event = &obj
	case events.SyslogE:
		var obj events.Syslog
		if err := json.Unmarshal(raw, &obj); err != nil {
			h.Counts.ParseErrs.Syslog++
			return nil, err
		}
		event = &obj
	case events.SnoopyE:
		var obj events.Snoopy
		if err := json.Unmarshal(raw, &obj); err != nil {
			h.Counts.ParseErrs.Snoopy++
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
		if rs, ok := h.sigma[event.Kind()]; ok {
			if res, match := rs.EvalAll(event); match && len(res) > 0 {
				fullAsset.MitreAttack.ParseSigmaTags(res, h.mitre.Mappings)
			}
		}
	}

	// add MITRE ATT&CK info
	if mitreInfo := event.GetMitreAttack(); mitreInfo != nil {
		mitreInfo.Set(h.mitre.Mappings)
		fullAsset.MitreAttack = mitreInfo
	}

	switch event.Kind() {
	case events.SuricataE:
		// TODO: our MITRE SID lookup
		// need alert SID, doubt theres any other way than typecasting...
	}

	// object is initialized with empty techniques, set nil if still empty for later emit check
	if len(fullAsset.MitreAttack.Techniques) == 0 {
		fullAsset.MitreAttack = nil
	}
	event.SetAsset(fullAsset)

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
			return val.Asset()
		}
		h.missingLookupSet[asset.Host] = true
	}
	return &asset
}

func (h *Handler) Close() error {
	return nil
}

func NewHandler(c Config) (*Handler, error) {
	if err := c.Validate(); err != nil {
		return nil, err
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
	m, err := mitre.NewMapper(c.Mitre)
	if err != nil {
		return nil, err
	}
	handler := &Handler{
		persist:          c.Persist,
		assets:           assets,
		mitre:            m,
		missingLookupSet: make(map[string]bool),
		Counts:           Counts{Assets: len(assets)},
	}
	if c.Sigma != nil && len(c.Sigma) > 0 {
		handler.sigma = c.Sigma
	}
	return handler, nil
}
