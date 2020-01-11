package meta

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
	Techniques []Technique
}
