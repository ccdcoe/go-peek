package meta

import (
	"strings"

	"github.com/markuskont/go-sigma-rule-engine"
)

type MitreGetter interface {
	GetMitreAttack() *MitreAttack
}

type MitreAttack struct {
	Technique
	Items      []string
	Techniques []Technique
}

func (m MitreAttack) Contains(key string) bool {
	for _, technique := range m.Techniques {
		if technique.Name == key {
			return true
		}
	}
	return false
}

func (m *MitreAttack) Update() *MitreAttack {
	if m.Techniques == nil || len(m.Techniques) < 1 {
		return m
	}
	m.Technique = m.Techniques[0]
	m.Items = make([]string, len(m.Techniques))
	for i, t := range m.Techniques {
		m.Items[i] = t.Name
	}
	return m
}

func (m *MitreAttack) Set(mapping Techniques) *MitreAttack {
	if m.Techniques == nil || len(m.Techniques) < 1 {
		return m
	}
	if mapping != nil {
		for i, t := range m.Techniques {
			if val, ok := mapping[t.ID]; ok {
				m.Techniques[i] = Technique{
					ID:     t.ID,
					Name:   val.Name,
					Phases: val.Phases,
					URL:    val.URL,
				}
			}
		}
	}
	return m.Update()
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
					if mapping == nil && !m.Contains(key) {
						m.Techniques = append(m.Techniques, Technique{ID: key})
					} else if val, ok := mapping[key]; ok && !m.Contains(key) {
						m.Techniques = append(m.Techniques, val)
					}
				}
			}
		}
	}
	return m.Update()
}

type Technique struct {
	ID     string
	Name   string
	URL    string
	Phases []string
}

type Techniques map[string]Technique
