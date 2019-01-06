package outputs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/ccdcoe/go-peek/internal/logging"
)

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
	Logger logging.LogHandler
}

func NewBulk(hosts []string, logs logging.LogHandler) *ElaBulk {
	var b = &ElaBulk{
		Hosts: hosts,
		Data:  make([][]byte, 0),
	}
	if logs == nil {
		b.Logger = logging.NewLogHandler()
	} else {
		b.Logger = logs
	}
	return b
}

func (b ElaBulk) Logs() logging.LogSender {
	return b.Logger
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
		resp, err := http.Post(
			b.Hosts[randProxy]+"/_bulk",
			"application/x-ndjson",
			buf,
		)
		if err != nil {
			b.Logger.Error(err)
		}
		if resp == nil {
			b.Logger.Error(
				fmt.Errorf("Missing response from bulk"),
			)
			return
		}
		b.Logger.Notify(resp.Body)
		resp.Body.Close()
	}(append(bytes.Join(
		b.Data,
		[]byte("\n"),
	)))
	b.Data = make([][]byte, 0)
	return b
}
