package oracle

import (
	"errors"
	"fmt"
	"net"
	"time"
)

var (
	ErrMissingIoCType  = errors.New("missing IoC type")
	ErrMissingIoCValue = errors.New("missing IoC value")
	ErrInvalidIP       = errors.New("invalid IP addr")
)

const (
	// update this whenever making changes to templates
	revision              = 3
	sidOffset             = 2000000000
	sidOffsetJa3Direction = 1000000
)

var (
	tplSrcIP    = `alert ip [%s] any -> $HOME_NET any (msg:"XS YT IoC - %s - %s - Known Bad IP Inbound Traffic"; threshold: type limit, track by_dst, seconds 60, count 1; classtype:misc-attack; flowbits:set,YT.Evil; sid:%d; rev:%d; metadata:%s;)`
	tplDestIP   = `alert ip $HOME_NET any -> [%s] any (msg:"XS YT IoC - %s - %s - Asset Connecting to Known Bad IP"; threshold: type limit, track by_src, seconds 60, count 1; classtype:misc-attack; flowbits:set,YT.Evil; sid:%d; rev:%d; metadata:%s;)`
	tplJA3      = `alert tls $HOME_NET any -> $EXTERNAL_NET any (msg:"XS YT IoC - %s - %s - Known Bad TLS Client Fingerprint Seen Outbound"; flow:established,to_server; flowbits:set,YT.Evil; ja3.hash; content:"%s"; classtype:trojan-activity; sid:%d; rev:%d; metadata:%s;)`
	tplJA3recon = `alert tls $EXTERNAL_NET any -> $HOME_NET any (msg:"XS YT IoC - %s - %s - Known Bad TLS Client Fingerprint Seen Inbound"; flow:established,to_server; flowbits:set,YT.Evil; ja3.hash; content:"%s"; classtype:trojan-activity; sid:%d; rev:%d; metadata:%s;)`
	tplJA3S     = `alert tls $EXTERNAL_NET any -> $HOME_NET any (msg:"XS YT IoC - %s - %s - Known Bad TLS Server Fingerprint Seen in Response"; flow:established,to_client; flowbits:set,YT.Evil; ja3s.hash; content:"%s"; classtype:trojan-activity; sid:%d; rev:%d; metadata:%s;)`
	tplTLSSNI   = `alert tls $HOME_NET any -> $EXTERNAL_NET any (msg:"XS YT IoC - %s - %s - Asset Connecting to Known Bad TLS Server"; flow:to_server,established; tls.sni; content:"%s"; endswith; flowbits:set,YT.Evil; classtype:domain-c2; sid:%d; rev:%d; metadata:%s;)`
	tplHTTPHost = `alert http $HOME_NET any -> $EXTERNAL_NET any (msg:"XS YT IoC - %s - %s - Asset Connecting to Known Bad HTTP Site"; flow:established,to_server; http.host; content:"%s"; endswith; flowbits:set,YT.Evil; classtype:trojan-activity; sid:%d; rev:%d; metadata: %s;)`

	tplMetadata           = `affected_product Any, attack_target Any, deployment Perimeter, tag YT, signature_severity Major, created_at 2021_11_30, updated_at 2021_11_30`
	tplMitreMetaEncrypted = `, mitre_tactic_id TA0011, mitre_tactic_name Command_And_Control, mitre_technique_id T1573, mitre_technique_name Encrypted_Channel`
)

func comment(base string, enabled bool) (tpl string) {
	tpl = base
	if !enabled {
		tpl = "# " + base
	}
	return tpl
}

func sid(i IoC) int { return sidOffset + i.ID }

func fmtRuleIP(tpl string, i IoC) string {
	return fmt.Sprintf(comment(tpl, i.Enabled),
		i.Value, i.Value, i.Type, sid(i), revision, tplMetadata)
}

func fmtRuleClientHash(i IoC) string {
	return fmtRuleEncryptedTraffic(tplJA3, i) +
		"\n" +
		fmt.Sprintf(comment(tplJA3recon, i.Enabled),
			i.Type, i.Value, i.Value, sid(i)+sidOffsetJa3Direction, revision, tplMetadata)
}

func fmtRuleEncryptedTraffic(tpl string, i IoC) string {
	return fmt.Sprintf(comment(tpl, i.Enabled),
		i.Type, i.Value, i.Value, sid(i), revision, tplMetadata)
}

func fmtRuleHTTPReq(tpl string, i IoC) string {
	return fmt.Sprintf(comment(tpl, i.Enabled),
		i.Type, i.Value, i.Value, sid(i), revision, tplMetadata)
}

type IndicatorOfCompromise int

// IoC stands for Indicator of compromise
type IoC struct {
	ID      int       `json:"id"`
	Enabled bool      `json:"enabled"`
	Value   string    `json:"value"`
	Type    string    `json:"type"`
	Added   time.Time `json:"added"`
}

func (i IoC) Rule() string {
	switch i.Type {
	case "src_ip":
		return fmtRuleIP(tplSrcIP, i)
	case "dest_ip":
		return fmtRuleIP(tplDestIP, i)
	case "tls.ja3.hash":
		return fmtRuleClientHash(i)
	case "tls.ja3s.hash":
		return fmtRuleEncryptedTraffic(tplJA3S, i)
	case "tls.sni":
		return fmtRuleEncryptedTraffic(tplTLSSNI, i)
	case "http.hostname":
		return fmtRuleHTTPReq(tplHTTPHost, i)
	default:
		return fmt.Sprintf("# unsupported ioc type for %d", i.ID)
	}
}

func (i IoC) key() string {
	return i.Type + "_" + i.Value
}

func (i IoC) assign(id int) IoC {
	return IoC{
		ID:      id,
		Enabled: i.Enabled,
		Value:   i.Value,
		Type:    i.Type,
		Added:   time.Now(),
	}
}

func (i IoC) copy() *IoC {
	return &IoC{
		ID:      i.ID,
		Enabled: i.Enabled,
		Value:   i.Value,
		Type:    i.Type,
		Added:   i.Added,
	}
}

func (i IoC) validate() error {
	if i.Type == "" {
		return ErrMissingIoCType
	}
	if i.Value == "" {
		return ErrMissingIoCValue
	}
	switch i.Type {
	case "src_ip", "dest_ip":
		if addr := net.ParseIP(i.Value); addr == nil {
			return ErrInvalidIP
		}
	}
	return nil
}

// IoCMapID stores IoC entries by ID for enable / disable
type IoCMapID map[int]*IoC

// IoCMap is for IoC entry to ensure unique item is created per type and value
type IoCMap map[string]*IoC

// Values is for reporting IoC list in GET and for exposing them to rule generator
func (i IoCMap) Values() []IoC {
	tx := make([]IoC, 0, len(i))
	for _, item := range i {
		tx = append(tx, *item)
	}
	return tx
}

func copyIocMap(rx IoCMap) IoCMap {
	tx := make(IoCMap)
	if rx == nil {
		return tx
	}
	for key, value := range rx {
		tx[key] = value.copy()
	}
	return tx
}

func copyIoCMapID(rx IoCMapID) IoCMapID {
	tx := make(IoCMapID)
	if rx == nil {
		return tx
	}
	for key, value := range rx {
		tx[key] = value.copy()
	}
	return tx
}
