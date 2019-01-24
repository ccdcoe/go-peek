package outputs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	if len(b.Data) == 0 {
		return b
	}
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
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			b.Logger.Error(err)
		}
		var decodedResp ElaResponse
		if err := json.Unmarshal(body, &decodedResp); err != nil {
			b.Logger.Error(err)
		}
		if err := decodedResp.checkErrs(); err != nil {
			b.Logger.Error(err)
		}
		resp.Body.Close()
	}(append(bytes.Join(
		b.Data,
		[]byte("\n"),
	)))
	b.Data = make([][]byte, 0)
	return b
}

type ElaResponse struct {
	Took   int  `json:"took"`
	Errors bool `json:"errors"`
	Items  []struct {
		Index struct {
			Index   string `json:"_index"`
			Type    string `json:"_type"`
			ID      string `json:"_id"`
			Version int    `json:"_version"`
			Result  string `json:"result"`
			Shards  struct {
				Total      int `json:"total"`
				Successful int `json:"successful"`
				Failed     int `json:"failed"`
			} `json:"_shards"`
			Error       *ElaResponseError `json:"error,omitempty"`
			SeqNo       int               `json:"_seq_no"`
			PrimaryTerm int               `json:"_primary_term"`
			Status      int               `json:"status"`
		} `json:"index"`
	} `json:"items"`
}

func (e ElaResponse) checkErrs() error {
	if e.Errors {
		for _, v := range e.Items {
			if err := v.Index.Error; err != nil {
				indexErr := &ErrIndex{
					err:   err,
					index: v.Index.Index,
					code:  v.Index.Status,
				}
				return indexErr
			}
		}
	}
	return nil
}

type ElaResponseError struct {
	Type     string `json:"type"`
	Reason   string `json:"reason"`
	CausedBy struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"caused_by"`
}

func (e ElaResponseError) Error() string {
	return fmt.Sprintf("Index err: [%s]. Type: [%s]. Reason: [%s]", e.Reason, e.Type, e.CausedBy.Reason)
}

type ErrIndex struct {
	err   error
	index string
	code  int
}

func (e ErrIndex) Error() string {
	return fmt.Sprintf("Index: %s, code: %d, err: %s", e.index, e.code, e.err)
}
