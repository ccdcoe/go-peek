package oracle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func (s *Server) handleIndex() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		get := map[string]string{
			"/":                      "API routes listing",
			"/assets":                "asset listing",
			"/mitremeerkat/mappings": "suricata SID to MITRE mappings",
			"/mitremeerkat/missing":  "missing suricata SID to MITRE mappings",
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
		mmWrapper(rw, r, &s.SidMap)
	}
}

func (s *Server) handleMitreMeerkatMissing() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		mmWrapper(rw, r, &s.MissingSidMaps)
	}
}

func (s *Server) handleIoCServe() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		encoded, err := json.Marshal(s.IoC.Slice())
		if err != nil {
			rw.WriteHeader(500)
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(200)
		rw.Write(encoded)
	}
}

func (s *Server) handleIoCAdd() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			rw.WriteHeader(500)
			fmt.Fprintf(rw, `{"status": "err", "error": "%s"}`, err)
			return
		}
		ioc := IoC{
			Type:  r.FormValue("type"),
			Value: r.FormValue("value"),
			Added: time.Now(),
		}
		id, err := s.IoC.Add(ioc)
		if err != nil {
			rw.WriteHeader(500)
			fmt.Fprintf(
				rw,
				`{"status": "err", "value": "%s", "error": "%s", "type": "%s"}`,
				ioc.Value, err, ioc.Type,
			)
			return
		}
		if id == -1 {
			rw.WriteHeader(500)
			fmt.Fprintf(
				rw,
				`{"status": "err", "value": "%s", "error": "unable to insert value", "type": "%s"}`,
				ioc.Value, ioc.Type,
			)
			return
		}

		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(200)
		fmt.Fprintf(
			rw,
			`{"status": "ok", "value": "%s", "type": "%s", "id": %d}`,
			ioc.Value,
			ioc.Type,
			id,
		)
	}
}
