package intel

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ccdcoe/go-peek/pkg/intel/wise"
	"github.com/ccdcoe/go-peek/pkg/models/meta"
)

var FieldPrefix = "peek"

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

func (a Asset) CSV() []string {
	return []string{
		fmt.Sprintf("%t", a.IsAsset),
		a.updated.String(),
		a.Data.Host,
		a.Data.Alias,
		a.Data.Kernel,
		a.Data.IP.String(),
	}
}

func (a Asset) JSON() ([]byte, error) { return json.Marshal(a) }

type Config struct {
	Wise           *wise.Config
	DumpJSONAssets string
	Prune          bool
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
