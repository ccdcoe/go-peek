package providentia

import (
	"encoding/json"
	"errors"
	"fmt"
	"go-peek/pkg/anonymizer"
	"go-peek/pkg/models"
	"go-peek/pkg/models/meta"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	ErrMissingToken = errors.New("Missing token")
	ErrMissingURL   = errors.New("Missing url")
)

type ErrRespDecode struct {
	Decode error
}

func (e ErrRespDecode) Error() string {
	return fmt.Sprintf("API response body decode: %s", e.Decode)
}

type Params struct {
	URL, Token string
	RawDump    string
}

type network struct {
	Name           string `json:"name"`
	ConnectionName string `json:"connection_name"`
	Domain         string `json:"domain"`

	IPv4 string `json:"ipv4"`
	IPv6 string `json:"ipv6"`
}

type MappedTarget struct {
	Target  Instance `json:"target"`
	Alias   string   `json:"alias"`
	Records []Record `json:"records"`
}

type Target struct {
	ID                      string        `json:"id"`
	Owner                   string        `json:"owner"`
	Description             string        `json:"description"`
	Role                    string        `json:"role"`
	TeamName                string        `json:"team_name"`
	BtVisible               bool          `json:"bt_visible"`
	HardwareCPU             int           `json:"hardware_cpu"`
	HardwareRAM             int           `json:"hardware_ram"`
	HardwarePrimaryDiskSize int           `json:"hardware_primary_disk_size"`
	Tags                    []string      `json:"tags"`
	Capabilities            []interface{} `json:"capabilities"`
	Services                []interface{} `json:"services"`
	SequenceTag             interface{}   `json:"sequence_tag"`
	SequenceTotal           interface{}   `json:"sequence_total"`
	Instances               []Instance    `json:"instances"`
}

type Instance struct {
	ID                string `json:"id"`
	VMName            string `json:"vm_name"`
	TeamUniqueID      string `json:"team_unique_id"`
	Hostname          string `json:"hostname"`
	Domain            string `json:"domain"`
	Fqdn              string `json:"fqdn"`
	ConnectionAddress string `json:"connection_address"`
	Interfaces        []struct {
		NetworkID  string `json:"network_id"`
		CloudID    string `json:"cloud_id"`
		Domain     string `json:"domain"`
		Fqdn       string `json:"fqdn"`
		Egress     bool   `json:"egress"`
		Connection bool   `json:"connection"`
		Addresses  []struct {
			PoolID     string `json:"pool_id"`
			Mode       string `json:"mode"`
			Connection bool   `json:"connection"`
			Address    string `json:"address"`
			DNSEnabled bool   `json:"dns_enabled"`
			Gateway    string `json:"gateway"`
		} `json:"addresses"`
	} `json:"interfaces"`
	Checks    []interface{} `json:"checks"`
	ConfigMap struct {
	} `json:"config_map"`
}

func (t Target) extractOS() string {
	if t.Tags == nil || len(t.Tags) == 0 {
		return ""
	}
	for _, group := range t.Tags {
		if strings.HasPrefix(group, "os_") {
			return group
		}
	}
	return "unk"
}

type Response struct {
	Result []Target `json:"result"`
}

type Records []Record

func (r Records) FilterByTime(since time.Duration) Records {
	tx := make(Records, 0, len(r))
	for _, item := range r {
		if time.Since(item.Updated) < since {
			tx = append(tx, item)
		}
	}
	return tx
}

type Record struct {
	AnsibleName string    `json:"ansible_name"`
	HostName    string    `json:"host_name"`
	Pretty      string    `json:"pretty"`
	Domain      string    `json:"domain"`
	Role        string    `json:"role"`
	Addr        net.IP    `json:"addr"`
	Team        string    `json:"team"`
	OS          string    `json:"os"`
	NetworkName string    `json:"network_name"`
	Updated     time.Time `json:"updated"`
	FQDN        string    `json:"fqdn,omitempty"`
}

func (r Record) IsAsset() bool { return r.Team == "blue" }

// VsphereCopy makes a new Record entry from vsphere asset report
// keep everything as-is, just copy higher-fidelity info with new IP
func (r Record) VsphereCopy(vs models.AssetVcenter) Record {
	return Record{
		AnsibleName: r.AnsibleName,
		HostName:    r.HostName,
		Pretty:      r.Pretty,
		Domain:      r.Domain,
		Role:        r.Role,
		Addr:        vs.IP.IP,
		Team:        r.Team,
		OS:          r.OS,
		NetworkName: r.NetworkName,
		Updated:     vs.TS,
		FQDN:        r.FQDN,
	}
}

func (r Record) Keys() []string {
	keys := []string{r.HostName, r.AnsibleName, r.Pretty, r.Addr.String()}
	if r.Domain != "" {
		keys = append(keys, r.FQDN)
	}
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		if key != "" {
			out = append(out, key)
		}
	}
	return out
}

func (r Record) Asset() *meta.Asset {
	return &meta.Asset{
		Host:   r.HostName,
		Alias:  r.Pretty,
		Domain: r.Domain,
		IP:     r.Addr,
		Team:   r.Team,
		VM:     r.AnsibleName,
		Role:   r.Role,
		OS:     r.OS,
		Zone:   r.NetworkName,
		Indicators: meta.Indicators{
			IsAsset: r.Team == "blue",
		},
	}
}

func Pull(p Params) ([]Target, error) {
	if p.URL == "" {
		return nil, ErrMissingURL
	}
	if p.Token == "" {
		return nil, ErrMissingToken
	}

	// Create a new request using http
	req, err := http.NewRequest("GET", p.URL, nil)
	if err != nil {
		return nil, err
	}

	// add authorization header to the req
	req.Header.Add("Authorization", p.Token)

	// Send req using http Client
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if p.RawDump != "" {
		f, err := os.Create(p.RawDump)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		f.Write(data)
	}

	var obj Response
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, ErrRespDecode{Decode: err}
	}
	return obj.Result, nil
}

func MapTargets(targets []Target, anon *anonymizer.Mapper) ([]MappedTarget, error) {
	tx := make([]MappedTarget, 0)
	err := ErrMissingInstances{Items: make([]Target, 0)}
	for _, tgt := range targets {
		if len(tgt.Instances) == 0 {
			err.Items = append(err.Items, tgt)
		}
		for _, instance := range tgt.Instances {
			alias, err := anon.CheckAndUpdate(instance.VMName)
			if err != nil {
				return tx, err
			}
			records := make([]Record, 0)
			for _, iface := range instance.Interfaces {
				for _, addr := range iface.Addresses {
					ip, _, err := net.ParseCIDR(addr.Address)
					if err != nil {
						return nil, err
					}
					if ip == nil {
						return nil, fmt.Errorf("unable to parse %s as IP", addr.Address)
					}
					var os string
					for _, tag := range tgt.Tags {
						if strings.HasPrefix(tag, "os_") {
							os = tag
						}
					}
					records = append(records, Record{
						AnsibleName: instance.ID,
						HostName:    instance.Hostname,
						Pretty:      alias,
						Domain:      instance.Domain,
						Role:        tgt.Role,
						Addr:        ip,
						OS:          os,
						Team:        tgt.TeamName,
						NetworkName: iface.NetworkID,
						Updated:     time.Now(),
						FQDN:        instance.Fqdn,
					})
				}
			}
			tx = append(tx, MappedTarget{
				Target:  instance,
				Alias:   alias,
				Records: records,
			})
		}
	}
	if len(err.Items) == 0 {
		return tx, nil
	}
	return tx, err
}
