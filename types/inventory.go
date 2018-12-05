package types

import (
	"encoding/json"
	"fmt"
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

func (t *ElaTargetInventory) ElaGet(host, index string) error {
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
	fmt.Println(s)

	if resp, err = http.Get(s + "/_search?size=200"); err != nil {
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

func (t ElaTargetInventory) MapKnownIP(ip2host map[string]string) {
	//return nil, nil
}
