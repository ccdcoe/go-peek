package mitre

import (
	"encoding/json"
	"errors"
	"fmt"
	"go-peek/pkg/models/meta"
	"go-peek/pkg/utils"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/sirupsen/logrus"
)

const src = `https://raw.githubusercontent.com/mitre/cti/master/enterprise-attack/enterprise-attack.json`

type Config struct {
	EnterpriseDump string
	MappingsDump   string
}

func (c *Config) Validate() error {
	if c.EnterpriseDump == "" {
		return errors.New("Mitre mapper missing dump file!")
	}
	return nil
}

type Mapper struct {
	c        Config
	Mappings meta.Techniques
}

func NewMapper(c Config) (*Mapper, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	var raw io.Reader
	var dumpIsPresent bool
	if utils.FileNotExists(c.EnterpriseDump) {
		logrus.Tracef(
			"Mitre enterprise dump %s does not exists, downloading from %s",
			c.EnterpriseDump,
			src,
		)
		response, err := http.Get(src)
		if err != nil {
			return nil, err
		}
		raw = response.Body
		defer response.Body.Close()
	} else {
		f, err := os.Open(c.EnterpriseDump)
		if err != nil {
			return nil, err
		}
		raw = f
		dumpIsPresent = true
		defer f.Close()
	}
	data, err := ioutil.ReadAll(raw)
	if err != nil {
		return nil, err
	}
	if !dumpIsPresent {
		f, err := os.Create(c.EnterpriseDump)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		if _, err := f.Write(data); err != nil {
			return nil, err
		}
	}
	mappings, err := parseEnterprise(data)
	if err != nil {
		return nil, err
	}
	if c.MappingsDump != "" {
		f, err := os.Create(c.MappingsDump)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		jsonMappings, err := json.Marshal(mappings)
		if err != nil {
			return nil, err
		}
		if _, err := f.Write(jsonMappings); err != nil {
			return nil, err
		}
	}
	return &Mapper{Mappings: mappings, c: c}, nil
}

type RawKillChainPhase struct {
	KillChainName string `json:"kill_chain_name"`
	PhaseName     string `json:"phase_name"`
}

type RawExternalReference struct {
	SourceName  string `json:"source_name"`
	ExternalID  string `json:"external_id"`
	URL         string `json:"url"`
	Description string `json:"description"`
}

type RawEnterpriseItem struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Revoked bool   `json:"revoked"`
	Type    string `json:"type"`

	KillChainPhases    []RawKillChainPhase    `json:"kill_chain_phases,omitempty"`
	ExternalReferences []RawExternalReference `json:"external_references"`
}

type RawEnterpriseJSON struct {
	Objects []RawEnterpriseItem `json:"objects"`
}

func parseEnterprise(input []byte) (meta.Techniques, error) {
	var rawJSON RawEnterpriseJSON
	if err := json.Unmarshal(input, &rawJSON); err != nil {
		return nil, fmt.Errorf("MITRE enterprise from %s parse fail %s", src, err)
	}
	out := make(meta.Techniques)
objects:
	for _, obj := range rawJSON.Objects {
		if obj.Type != "attack-pattern" {
			continue objects
		}
		if obj.Revoked {
			continue objects
		}
	references:
		for _, ref := range obj.ExternalReferences {
			if ref.SourceName != "mitre-attack" {
				continue references
			}
			out[ref.ExternalID] = meta.Technique{
				ID:   ref.ExternalID,
				Name: obj.Name,
				URL:  ref.URL,
				Phases: func() []string {
					if obj.KillChainPhases == nil || len(obj.KillChainPhases) == 0 {
						return nil
					}
					tx := make([]string, 0)
				phases:
					for _, phase := range obj.KillChainPhases {
						if phase.KillChainName != "mitre-attack" {
							continue phases
						}
						tx = append(tx, phase.PhaseName)
					}
					return tx
				}(),
			}
		}
	}
	return out, nil
}
