package wise

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/ccdcoe/go-peek/pkg/models/meta"
	"github.com/ccdcoe/go-peek/pkg/utils"
)

var FieldPrefix = "peek"

type Buffer struct {
	Field  string `json:"field"`
	Value  string `json:"value"`
	Length int    `json:"len"`
}

type APIResponse []Buffer

func (r APIResponse) Map() map[string]string {
	out := make(map[string]string)
	for _, resp := range r {
		out[resp.Field] = resp.Value
	}
	return out
}

type Plugin int

const (
	PluginAll Plugin = iota
	PluginRedis
	PluginFile
	PluginReverseDNS
)

type Config struct {
	Host string
	Plugin
}

func (c *Config) Validate() error {
	return nil
}

type Handle struct {
	alive  bool
	url    url.URL
	client http.Client
	Plugin
}

func NewHandle(c *Config) (*Handle, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	h := &Handle{
		Plugin: c.Plugin,
		client: http.Client{
			//Timeout: 500 * time.Millisecond,
		},
	}
	if u, err := url.Parse(c.Host); err != nil {
		return nil, err
	} else {
		h.url = *u
	}
	if _, err := QueryIP(*h, net.ParseIP("8.8.8.8")); err != nil {
		h.alive = false
		return h, err
	}
	return h, nil
}

func QueryIP(h Handle, key net.IP) (APIResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	req, err := http.NewRequest("GET", h.url.String()+"/ip/"+key.String(), nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	// TODO - count timeouts for metrix
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var data APIResponse
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, utils.ErrDecodeJson{
			Err: err,
			Raw: body,
		}
	}
	return data, nil
}

func GetAsset(h Handle, key net.IP, hostKey, aliasKey, kernelKey string) (*meta.Asset, bool, error) {
	resp, err := QueryIP(h, key)
	if err != nil {
		return nil, false, err
	}
	if len(resp) > 0 {
		m := &meta.Asset{IP: key, Indicators: meta.Indicators{
			IsAsset: true,
		}}
		for _, field := range resp {
			if hostKey != "" && field.Field == hostKey {
				m.Host = field.Value
			}
			if aliasKey != "" && field.Field == aliasKey {
				m.Alias = field.Value
			}
			if kernelKey != "" && field.Field == kernelKey {
				m.Kernel = field.Value
			}
		}
		return m, true, nil
	}
	return nil, false, nil
}

type callResp struct {
	Resp APIResponse
	Err  error
}
