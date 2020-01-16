package meta

import "fmt"

type Tactic struct {
	ID   string
	Name string
}

type Technique struct {
	ID      string
	Name    string
	Tactics []Tactic
}

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
