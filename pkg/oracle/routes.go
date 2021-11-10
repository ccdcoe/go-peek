package oracle

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// Routes set up API endpoints
func (s *Server) Routes() {
	s.Router = mux.NewRouter()

	s.Router.HandleFunc("/", s.handleIndex())
	s.Router.HandleFunc("/assets", s.handleAssets())
	s.Router.HandleFunc("/mitremeerkat", s.handleAssets())
}

func (s *Server) handleIndex() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		get := map[string]string{
			"/":             "API routes listing",
			"/assets":       "asset listing",
			"/mitremeerkat": "suricata SID to MITRE mappings",
		}
		d, err := json.Marshal(get)
		if err != nil {
			rw.WriteHeader(500)
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(200)
		rw.Write(d)
	}
}

func (s *Server) handleAssets() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		data, err := s.Assets.JSON()
		if err != nil {
			rw.WriteHeader(500)
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(200)
		rw.Write(data)
	}
}

func (s *Server) handleMitreMeerkat() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
	}
}
