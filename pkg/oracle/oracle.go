package oracle

import (
	"go-peek/pkg/mitremeerkat"
	"go-peek/pkg/providentia"

	"github.com/gorilla/mux"
)

type Data struct {
	Assets  map[string]providentia.Record
	Meerkat map[int]mitremeerkat.Mapping
}

func NewData() *Data {
	return &Data{
		Assets:  make(map[string]providentia.Record),
		Meerkat: make(map[int]mitremeerkat.Mapping),
	}
}

type Server struct {
	Router *mux.Router

	Assets ContainerAssets
	SidMap ContainerMitreMeerkat
}
