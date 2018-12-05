package outputs

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"net/http"
)

type ElaBulk struct {
	Data   [][]byte
	Hosts  []string
	Resps  chan *http.Response
	errors chan error
}

func NewBulk(hosts []string) *ElaBulk {
	return &ElaBulk{
		Hosts:  hosts,
		Data:   make([][]byte, 0),
		Resps:  make(chan *http.Response, 256),
		errors: make(chan error, 256),
	}
}

func (b ElaBulk) Errors() <-chan error {
	return b.errors
}

func (b *ElaBulk) AddIndex(item []byte, index string, id ...string) *ElaBulk {
	var m = map[string]map[string]string{
		"index": {
			"_index": index,
			"_type":  "doc",
		},
	}
	if len(id) > 0 {
		m["index"]["_id"] = id[0]
	}
	meta, _ := json.Marshal(m)
	b.Data = append(b.Data, meta)
	b.Data = append(b.Data, item)
	return b
}

func (b *ElaBulk) Flush() *ElaBulk {
	go func(data []byte) {
		buf := bytes.NewBuffer(data)
		buf.WriteRune('\n')
		randProxy := rand.Int() % len(b.Hosts)
		resp, err := http.Post(b.Hosts[randProxy]+"/_bulk", "application/x-ndjson", buf)
		if err != nil {
			if len(b.errors) == 256 {
				<-b.errors
			}
			b.errors <- err
		}
		if resp != nil {
			resp.Body.Close()
			if len(b.Resps) == 256 {
				<-b.Resps
			}
			b.Resps <- resp
		}
	}(append(bytes.Join(b.Data, []byte("\n"))))
	b.Data = make([][]byte, 0)
	return b
}
