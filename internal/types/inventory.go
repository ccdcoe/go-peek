package types

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"net/url"
	"path"
)

type ElaGrainSource struct {
	Index  string     `json:"_index"`
	Type   string     `json:"_type"`
	ID     string     `json:"_id"`
	Score  float64    `json:"_score"`
	Source *SaltGrain `json:"_source"`
}

type ElaTargetInventoryConfig struct {
	Hosts []string
	Index string
}

type ElaTargetInventory struct {
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
	Shards   struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Skipped    int `json:"skipped"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	Hits struct {
		Total    int               `json:"total"`
		MaxScore float64           `json:"max_score"`
		Hits     []*ElaGrainSource `json:"hits"`
	} `json:"hits"`
}

func NewElaTargetInventory() *ElaTargetInventory {
	return &ElaTargetInventory{}
}

func (t *ElaTargetInventory) Get(config ElaTargetInventoryConfig) error {
	var (
		err       error
		resp      *http.Response
		u         *url.URL
		randProxy int
	)

	randProxy = rand.Int() % len(config.Hosts)
	if u, err = url.Parse(config.Hosts[randProxy]); err != nil {
		return err
	}
	u.Path = path.Join(u.Path, config.Index)
	s := u.String()

	if resp, err = http.Get(s + "/_search?size=400"); err != nil {
		return err
	}
	defer resp.Body.Close()
	var decoder = json.NewDecoder(resp.Body)
	if err = decoder.Decode(t); err != nil {
		return err
	}

	return nil
}

func (t ElaTargetInventory) JSON() ([]byte, error) {
	return json.Marshal(t)
}
