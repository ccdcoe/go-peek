package oracle

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
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
			Enabled: true,
			Type:    r.FormValue("type"),
			Value:   r.FormValue("value"),
			Added:   time.Now(),
		}
		id, err := s.IoC.Add(ioc, false)
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

// FIXME - refactor along with enable
func (s *Server) handleIoCDisable() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		id, ok := vars["id"]
		if !ok {
			simpleJSONErr(errors.New("missing id"), rw)
			return
		}
		num, err := strconv.Atoi(id)
		if err != nil {
			simpleJSONErr(err, rw)
			return
		}
		item, err := s.IoC.Disable(num)
		if err != nil {
			simpleJSONErr(err, rw)
			return
		}
		respEncodeJSON(rw, item)
	}
}

// FIXME - refactor along with Disable
func (s *Server) handleIoCEnable() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		id, ok := vars["id"]
		if !ok {
			simpleJSONErr(errors.New("missing id"), rw)
			return
		}
		num, err := strconv.Atoi(id)
		if err != nil {
			simpleJSONErr(err, rw)
			return
		}
		item, err := s.IoC.Enable(num)
		if err != nil {
			simpleJSONErr(err, rw)
			return
		}
		respEncodeJSON(rw, item)
	}
}

// update this whenever making changes to templates
const revision = 1

var (
	tplSrcIP  = `alert ip [%s/32] any -> $HOME_NET any (msg:"XS YT IoC for %s"; threshold: type limit, track by_src, seconds 3600, count 1; classtype:misc-attack; flowbits:set,YT.Evil; sid:%d; rev:%d; metadata:affected_product Any, attack_target Any, deployment Perimeter, tag YT, signature_severity Major, created_at 2021_11_30, updated_at 2021_11_30;)`
	tplDestIP = `alert ip $HOME_NET any -> [%s/32] any (msg:"XS YT IoC for %s"; threshold: type limit, track by_src, seconds 3600, count 1; classtype:misc-attack; flowbits:set,YT.Evil; sid:%d; rev:%d; metadata:affected_product Any, attack_target Any, deployment Perimeter, tag YT, signature_severity Major, created_at 2021_11_30, updated_at 2021_11_30;)`
)

func (s *Server) handleIoCMeerkatRules() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		ioc := s.IoC.Extract()
		offset := 2000000000
		for _, item := range ioc {
			switch item.Type {
			case "src_ip":
				fmt.Fprintf(rw, template(tplSrcIP, item.Enabled), item.Value, item.Type, offset+item.ID, revision)
				fmt.Fprintf(rw, "\n")
			case "dest_ip":
				fmt.Fprintf(rw, template(tplDestIP, item.Enabled), item.Value, item.Type, offset+item.ID, revision)
				fmt.Fprintf(rw, "\n")
			}
		}
	}
}

func template(base string, enabled bool) (tpl string) {
	tpl = base
	if !enabled {
		tpl = "# " + base
	}
	return tpl
}
