package mitre

import (
	"testing"

	"github.com/sirupsen/logrus"
)

func TestMitre(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)
	_, err := NewMapper(Config{
		EnterpriseDump: "/tmp/mitre-enterprise.json",
		MappingsDump:   "/tmp/mitre-mappings.json",
	})
	if err != nil {
		t.Fatal(err)
	}
}
