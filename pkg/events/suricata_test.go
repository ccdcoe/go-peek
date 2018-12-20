package events

import (
	"encoding/json"
	"fmt"
	"testing"
)

var surilog = ``

func TestSmbParse(t *testing.T) {
	var m Eve
	if err := json.Unmarshal([]byte(surilog), &m); err != nil {
		t.Fail()
	}
	back, _ := m.JSON()
	fmt.Println(string(back))
}
