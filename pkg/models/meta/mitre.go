package meta

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/ccdcoe/go-peek/pkg/utils"
	"github.com/markuskont/go-sigma-rule-engine/pkg/sigma"
)

const src = `https://raw.githubusercontent.com/mitre/cti/master/enterprise-attack/enterprise-attack.json`

type Tactic struct {
	ID   string
	Name string
}

/*
type Technique struct {
	ID      string
	Name    string
	Tactics []Tactic
}
*/

type MitreAttack struct {
	Technique
	Items      []string
	Techniques []Technique
}

func (m *MitreAttack) Set() *MitreAttack {
	if m.Techniques != nil && len(m.Techniques) > 0 {
		m.Name = fmt.Sprintf("%s: %s", m.Techniques[0].ID, m.Techniques[0].Name)
		m.ID = m.Techniques[0].ID
		m.Items = make([]string, len(m.Techniques))
		for i, t := range m.Techniques {
			m.Items[i] = t.Name
		}
	}
	return m
}

func (m *MitreAttack) ParseSigmaTags(results sigma.Results, mapping Techniques) *MitreAttack {
	if results == nil || len(results) == 0 {
		return m
	}

	for _, res := range results {
		for _, tag := range res.Tags {
			if strings.HasPrefix(tag, "attack.t") {
				if id := strings.Split(tag, "."); len(id) == 2 && len(id[1]) == 5 {
					key := strings.ToUpper(id[1])
					if mapping == nil {
						m.Techniques = append(m.Techniques, Technique{ID: key})
					} else if val, ok := mapping[key]; ok {
						m.Techniques = append(m.Techniques, Technique{
							ID:     key,
							Name:   val.Name,
							Phases: val.Phases,
						})
					}
				}
			}
		}
	}
	return m
}

type Technique struct {
	ID     string
	Name   string
	Phases []string
}

type Techniques map[string]Technique

func NewTechniquesFromJSONfile(path string) (Techniques, error) {
	path, err := utils.ExpandHome(path)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Print(err)
	}
	var t Techniques
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, err
	}
	return t, nil
}
