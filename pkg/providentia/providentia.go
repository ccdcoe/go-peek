package providentia

import (
	"encoding/json"
	"errors"
	"fmt"
	"go-peek/pkg/anonymizer"
	"go-peek/pkg/models/meta"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
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

type Targets []Target

type MappedTarget struct {
	Target Target `json:"target"`
	Alias  string `json:"alias"`
}

type Target struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Owner string `json:"owner"`
	Team  struct {
		Name      string `json:"name"`
		BTVisible bool   `json:"bt_visible"`
	} `json:"team"`

	Vars struct {
		ID       string    `json:"id"`
		Role     string    `json:"role"`
		Hostname string    `json:"hostname"`
		Networks []network `json:"networks"`
		Groups   []string  `json:"groups"`
	} `json:"vars"`

	Groups []string `json:"groups,omitempty"`
}

func (t Target) extractOS() string {
	if t.Groups == nil || len(t.Groups) == 0 {
		return ""
	}
	for _, group := range t.Groups {
		if strings.HasPrefix(group, "os_") {
			return group
		}
	}
	return "unk"
}

func (t Target) extract(alias string, logger *logrus.Logger) []Record {
	tx := make([]Record, 0)
	for _, nw := range t.Vars.Networks {
		if nw.IPv4 != "" {
			if addr, _, err := net.ParseCIDR(nw.IPv4); err == nil {
				tx = append(tx, Record{
					AnsibleName: t.Name,
					HostName:    t.Vars.Hostname,
					Role:        t.Vars.Role,
					Pretty:      alias,
					Domain:      nw.Domain,
					Updated:     time.Now(),
					Addr:        addr,
					Team:        t.Team.Name,
					OS:          t.extractOS(),
					NetworkName: nw.Name,
				})
			} else if logger != nil {
				logger.WithFields(logrus.Fields{
					"ip":   nw.IPv4,
					"name": nw.Name,
				}).Error(err)
			}
		}
		if nw.IPv6 != "" {
			if addr, _, err := net.ParseCIDR(nw.IPv6); err == nil {
				tx = append(tx, Record{
					AnsibleName: t.Name,
					HostName:    t.Vars.Hostname,
					Role:        t.Vars.Role,
					Pretty:      alias,
					Domain:      nw.Domain,
					Updated:     time.Now(),
					Addr:        addr,
					Team:        t.Team.Name,
					OS:          t.extractOS(),
					NetworkName: nw.Name,
				})
			} else if logger != nil {
				logger.WithFields(logrus.Fields{
					"ip":   nw.IPv6,
					"name": nw.Name,
				}).Error(err)
			}
		}
	}
	return tx
}

type Response struct {
	Hosts []Target `json:"hosts"`
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
}

func (r Record) FQDN() string { return r.HostName + "." + r.Domain }

func (r Record) Keys() []string {
	keys := []string{r.HostName, r.AnsibleName, r.Pretty, r.Addr.String()}
	if r.Domain != "" {
		keys = append(keys, r.FQDN())
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

type Records []Record

func Pull(p Params) (Targets, error) {
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
	return obj.Hosts, nil
}

func ExtractAddrs(targets []MappedTarget, logger *logrus.Logger) Records {
	tx := make(Records, 0)
	for _, target := range targets {
		tx = append(tx, target.Target.extract(target.Alias, logger)...)
	}
	return tx
}

func MapTargets(targets Targets, anon *anonymizer.Mapper) []MappedTarget {
	tx := make([]MappedTarget, len(targets))
	for i, tgt := range targets {
		tx[i] = MappedTarget{
			Target: tgt,
			Alias:  anon.CheckAndUpdate(tgt.Name),
		}
	}
	return tx
}
