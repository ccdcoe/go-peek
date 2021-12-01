package oracle

import (
	"github.com/gorilla/mux"
)

// Routes set up API endpoints
func (s *Server) Routes() {
	s.Router = mux.NewRouter()

	s.Router.HandleFunc("/", s.handleIndex())
	s.Router.HandleFunc("/assets", s.handleAssets()).Methods("GET")
	s.Router.HandleFunc("/mitremeerkat/mappings", s.handleMitreMeerkat()).Methods("GET")
	s.Router.HandleFunc("/mitremeerkat/missing", s.handleMitreMeerkatMissing()).Methods("GET")
	s.Router.HandleFunc("/ioc", s.handleIoCServe()).Methods("GET")
	s.Router.HandleFunc("/ioc", s.handleIoCAdd()).Methods("POST")
	s.Router.HandleFunc("/ioc/{id}", s.handleIoCDisable()).Methods("DELETE")
	s.Router.HandleFunc("/ioc/{id}", s.handleIoCEnable()).Methods("PUT")
	s.Router.HandleFunc("/ioc/rules", s.handleIoCMeerkatRules()).Methods("GET")
}
