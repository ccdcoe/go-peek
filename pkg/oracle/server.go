package oracle

import (
	"github.com/gorilla/mux"
)

type Server struct {
	Router *mux.Router

	Assets ContainerAssets

	SidMap         ContainerMitreMeerkat
	MissingSidMaps ContainerMitreMeerkat

	IoC ContainerIoC
}
