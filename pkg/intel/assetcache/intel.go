package assetcache

import (
	"encoding/json"
	"time"

	"github.com/ccdcoe/go-peek/pkg/intel/wise"
	"github.com/ccdcoe/go-peek/pkg/models/meta"
)

var FieldPrefix = "target"

// Asset is a wrapper around meta.Asset, with cache specific helper fields and methods
type Asset struct {
	Data    *meta.Asset
	updated time.Time
	IsAsset bool
}

func (a Asset) Update() *Asset {
	a.updated = time.Now()
	return &a
}

func (a Asset) JSON() ([]byte, error) { return json.Marshal(a) }

type Config struct {
	Wise           *wise.Config
	Prune          bool
	DumpJSONAssets string
	LoadJSONnets   string
}

func NewDefaultConfig() *Config {
	return &Config{}
}

func (c *Config) Validate() error {
	if c == nil {
		c = NewDefaultConfig()
	}
	return nil
}

type prune struct {
	enabled  bool
	interval time.Duration
	period   time.Duration
}
