package meta

import (
	"fmt"
	"testing"

	"github.com/markuskont/go-sigma-rule-engine/pkg/sigma"
)

func TestSigmaToMitre(t *testing.T) {
	tags := sigma.Tags([]string{
		"attack.t1190",
		"attack.t1063",
		"attack.t1046",
		"attack.initial_access",
		"attack.discovery",
	})
	a := &MitreAttack{}
	r := sigma.Result{
		Tags: tags,
	}
	a.ParseSigmaTags([]sigma.Result{r}, nil)
	if len(a.Techniques) != 3 {
		t.Fatalf("Sigma to Mitre test should output three items")
	}
}
func TestSigmaToMitreWithMapping(t *testing.T) {
	tags := sigma.Tags([]string{
		"attack.t1190",
		"attack.t1063",
		"attack.t1046",
		"attack.initial_access",
		"attack.discovery",
	})
	a := &MitreAttack{}
	r := sigma.Result{
		Tags: tags,
	}
	mapping, err := NewTechniquesFromJSONfile("~/.local/peek/mitre.json")
	if err != nil {
		t.Fatal(err)
	}
	a.ParseSigmaTags([]sigma.Result{r}, mapping)
	if len(a.Techniques) != 3 {
		t.Fatalf("Sigma to Mitre test should output three items")
	}
	for _, tech := range a.Techniques {
		fmt.Println(tech)
	}
}
