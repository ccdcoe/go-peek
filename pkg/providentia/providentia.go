package providentia

import (
	"encoding/json"
	"errors"
	"fmt"
	"go-peek/pkg/anonymizer"
	"net"
	"net/http"
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
	Logger     *logrus.Logger
	Anonymizer *anonymizer.Mapper
}

type network struct {
	Name           string `json:"name"`
	ConnectionName string `json:"connection_name"`
	Domain         string `json:"domain"`

	IPv4 string `json:"ipv4"`
	IPv6 string `json:"ipv6"`
}

type target struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Owner string `json:"owner"`
	Team  struct {
		Name      string `json:"name"`
		BTVisible bool   `json:"bt_visible"`
	} `json:"team"`

	Vars struct {
		Networks []network `json:"networks"`
		Groups   []string  `json:"groups"`
	} `json:"vars"`
}

func (t target) extract(logger *logrus.Logger, anon *anonymizer.Mapper) []Record {
	var pretty string
	if anon != nil {
		pretty = anon.CheckAndUpdate(t.Name)
	}
	tx := make([]Record, 0)
	for _, nw := range t.Vars.Networks {
		if nw.IPv4 != "" {
			if addr, _, err := net.ParseCIDR(nw.IPv4); err == nil {
				tx = append(tx, Record{
					Name:    t.Name,
					Pretty:  pretty,
					Domain:  nw.Domain,
					Updated: time.Now(),
					Addr:    addr,
					Team:    t.Team.Name,
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
					Name:    t.Name,
					Pretty:  pretty,
					Domain:  nw.Domain,
					Updated: time.Now(),
					Addr:    addr,
					Team:    t.Team.Name,
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

type response struct {
	Hosts []target `json:"hosts"`
}

func (r response) extract(logger *logrus.Logger, anon *anonymizer.Mapper) Records {
	tx := make([]Record, 0)
	for _, host := range r.Hosts {
		tx = append(tx, host.extract(logger, anon)...)
	}
	return tx
}

type Record struct {
	Name    string    `json:"name"`
	Pretty  string    `json:"pretty"`
	Domain  string    `json:"domain"`
	Addr    net.IP    `json:"addr"`
	Team    string    `json:"team"`
	Updated time.Time `json:"updated"`
}

type Records []Record

func Pull(p Params) (Records, error) {
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
	var obj response
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&obj); err != nil {
		return nil, ErrRespDecode{Decode: err}
	}
	return obj.extract(p.Logger, p.Anonymizer), nil
}