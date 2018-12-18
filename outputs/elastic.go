package outputs

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

const errBufSize = 256
const respBufSize = 256

type ElaIndex string

func (e ElaIndex) Format(timestamp time.Time) string {
	return strings.Join([]string{e.String(), timestamp.Format(e.Hourly())}, "-")
}
func (e ElaIndex) Hourly() string { return "2006.01.02.15" }
func (e ElaIndex) Daily() string  { return "2006.01.02" }
func (e ElaIndex) String() string { return string(e) }

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
		Resps:  make(chan *http.Response, respBufSize),
		errors: make(chan error, errBufSize),
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
			if len(b.errors) == errBufSize {
				<-b.errors
			}
			b.errors <- err
		}
		if resp != nil {
			resp.Body.Close()
			if len(b.Resps) == respBufSize {
				<-b.Resps
			}
			b.Resps <- resp
		}
	}(append(bytes.Join(b.Data, []byte("\n"))))
	b.Data = make([][]byte, 0)
	return b
}
