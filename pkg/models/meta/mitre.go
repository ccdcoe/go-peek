package meta

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

func (m *MitreAttack) Update() {
	if m.Techniques == nil || len(m.Techniques) < 1 {
		return
	}
	// set to deduplicate values
	seen := make(map[string]bool)
	// new slice to deduplicate values
	techniques := make([]Technique, 0, len(m.Techniques))
	// shorthand list of technique names
	items := make([]string, 0, len(m.Techniques))

loop:
	for _, t := range m.Techniques {
		if seen[t.ID] {
			continue loop
		}
		seen[t.ID] = true
		items = append(items, t.Name)
		techniques = append(techniques, t)
	}
	m.Items = items
	m.Techniques = techniques
	m.Technique = m.Techniques[0]
}

func (m *MitreAttack) Set(mapping Techniques) {
	if m.Techniques == nil || len(m.Techniques) < 1 {
		return
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
}

type Technique struct {
	ID     string
	Name   string
	URL    string
	Phases []string
}

type Techniques map[string]Technique
