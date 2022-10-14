package enrich

import (
	"go-peek/pkg/models/meta"
	"strings"
)

func parseMitreTags(tags []string, mappings meta.Techniques) []meta.Technique {
	if len(tags) == 0 {
		return nil
	}
	tx := make([]meta.Technique, 0, len(tags))
loop:
	for _, tag := range tags {
		if !strings.HasPrefix("attack.t", tag) {
			continue loop
		}
		id := strings.SplitN(tag, ".", 2)
		if len(id) != 2 {
			continue loop
		}
		t, ok := mappings[strings.ToUpper(id[1])]
		if !ok {
			continue loop
		}
		tx = append(tx, t)
	}
	if len(tx) == 0 {
		return nil
	}
	return tx
}
