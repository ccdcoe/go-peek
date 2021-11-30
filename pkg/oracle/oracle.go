package oracle

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"go-peek/pkg/mitremeerkat"
	"go-peek/pkg/models/atomic"
	"go-peek/pkg/providentia"
	"net/http"
)

type Data struct {
	Assets         map[string]providentia.Record
	Meerkat        map[int]mitremeerkat.Mapping
	MissingSidMaps mitremeerkat.Mappings
	IoC            []IoC
}

func NewData() *Data {
	return &Data{
		Assets:         make(map[string]providentia.Record),
		Meerkat:        make(map[int]mitremeerkat.Mapping),
		MissingSidMaps: make(mitremeerkat.Mappings, 0),
	}
}

func respEncodeJSON(rw http.ResponseWriter, data interface{}) {
	encoded, err := json.Marshal(data)
	if err != nil {
		simpleJSONErr(err, rw)
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(200)
	rw.Write(encoded)
}

func respJSON(rw http.ResponseWriter, data atomic.JSONFormatter) {
	encoded, err := data.JSONFormat()
	if err != nil {
		rw.WriteHeader(500)
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(200)
	rw.Write(encoded)
}

func respCSV(rw http.ResponseWriter, data atomic.CSVFormatter) {
	rw.Header().Set("Content-Type", "text/x-csv")
	rw.WriteHeader(200)

	rows := data.CSVFormat(true)

	w := csv.NewWriter(rw)
	w.WriteAll(rows)
}

func mmWrapper(rw http.ResponseWriter, r *http.Request, data *ContainerMitreMeerkat) {
	format := r.URL.Query().Get("format")
	switch format {
	case "csv":
		respCSV(rw, data)
	default:
		respJSON(rw, data)
	}
}

func simpleJSONErr(err error, rw http.ResponseWriter) {
	fmt.Fprintf(rw, `{"status": "err", "error": "%s"}`, err)
}
