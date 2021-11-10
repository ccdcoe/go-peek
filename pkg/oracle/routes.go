package oracle

import (
	"github.com/gorilla/mux"
)

// Routes set up API endpoints
func (s *Server) Routes() {
	s.Router = mux.NewRouter()

	s.Router.HandleFunc("/", s.handleIndex())
	s.Router.HandleFunc("/assets", s.handleAssets())
	s.Router.HandleFunc("/mitremeerkat", s.handleMitreMeerkat())
}
