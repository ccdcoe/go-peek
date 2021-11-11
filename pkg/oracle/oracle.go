package oracle

import (
	"go-peek/pkg/mitremeerkat"
	"go-peek/pkg/models/atomic"
	"go-peek/pkg/providentia"
	"net/http"

	"github.com/gorilla/mux"
)

type Data struct {
	Assets         map[string]providentia.Record
	Meerkat        map[int]mitremeerkat.Mapping
	MissingSidMaps mitremeerkat.Mappings
}

func NewData() *Data {
	return &Data{
		Assets:         make(map[string]providentia.Record),
		Meerkat:        make(map[int]mitremeerkat.Mapping),
		MissingSidMaps: make(mitremeerkat.Mappings, 0),
	}
}

type Server struct {
	Router *mux.Router

	Assets ContainerAssets

	SidMap         ContainerMitreMeerkat
	MissingSidMaps ContainerMitreMeerkat
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
