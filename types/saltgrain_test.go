package types

import (
	"encoding/json"
	"fmt"
	"testing"
)

var grain = ``

func TestGrainParse(t *testing.T) {
	var m SaltGrain
	if err := json.Unmarshal([]byte(grain), &m); err != nil {
		fmt.Println(err)
		t.Fail()
	}
	fmt.Println(m)
}
