package types

import (
	"encoding/json"
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

func (t *ElaTargetInventory) Get(host, index string) error {
	var (
		err  error
		resp *http.Response
		u    *url.URL
	)

	if u, err = url.Parse(host); err != nil {
		return err
	}
	u.Path = path.Join(u.Path, index)
	s := u.String()

	if resp, err = http.Get(s + "/_search?size=400"); err != nil {
		return err
	}
	defer resp.Body.Close()
	var decoder = json.NewDecoder(resp.Body)
	if err := decoder.Decode(t); err != nil {
		return err
	}

	return nil
}

func (t ElaTargetInventory) JSON() ([]byte, error) {
	return json.Marshal(t)
}

func (t ElaTargetInventory) MapKnownIP(ip2pretty map[string]string) map[string]string {
	var mapped = make(map[string]string)
	for _, grain := range t.Hits.Hits {
		addrs := grain.Source.GetAddrs()
		for k, v := range ip2pretty {
			if addrs.ContainS(k) {
				for _, a := range addrs {
					mapped[a.String()] = v
				}
			}
		}
	}

	return mapped
}
