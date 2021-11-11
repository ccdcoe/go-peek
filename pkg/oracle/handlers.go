package oracle

import (
	"encoding/json"
	"net/http"
)

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
		respJSON(rw, &s.Assets)
	}
}

func (s *Server) handleMitreMeerkat() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		respJSON(rw, &s.SidMap)
	}
}

func (s *Server) handleMitreMeerkatMissing() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		fmt := r.URL.Query().Get("format")
		if fmt == "csv" {
			respCSV(rw, &s.MissingSidMaps)
			return
		}
		respJSON(rw, &s.MissingSidMaps)
	}
}
