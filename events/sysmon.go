package events

import (
	"encoding/json"
	"time"
)

type Sysmon struct {
	Syslog
	EventTime         string `json:"EventTime"`
	Hostname          string `json:"Hostname"`
	Keywords          int64  `json:"Keywords"`
	EventType         string `json:"EventType"`
	SeverityValue     int    `json:"SeverityValue"`
	Severity          string `json:"Severity"`
	EventID           int    `json:"EventID"`
	SourceName        string `json:"SourceName"`
	ProviderGUID      string `json:"ProviderGuid"`
	Version           int    `json:"Version"`
	Task              int    `json:"Task"`
	OpcodeValue       int    `json:"OpcodeValue"`
	RecordNumber      int    `json:"RecordNumber"`
	ProcessID         int    `json:"ProcessID"`
	ThreadID          int    `json:"ThreadID"`
	Channel           string `json:"Channel"`
	Domain            string `json:"Domain"`
	AccountName       string `json:"AccountName"`
	UserID            string `json:"UserID"`
	AccountType       string `json:"AccountType"`
	Message           string `json:"Message"`
	Category          string `json:"Category"`
	Opcode            string `json:"Opcode"`
	UtcTime           string `json:"UtcTime"`
	ProcessGUID       string `json:"ProcessGuid"`
	Image             string `json:"Image"`
	FileVersion       string `json:"FileVersion"`
	Description       string `json:"Description"`
	Product           string `json:"Product"`
	Company           string `json:"Company"`
	CommandLine       string `json:"CommandLine"`
	CurrentDirectory  string `json:"CurrentDirectory"`
	User              string `json:"User"`
	LogonGUID         string `json:"LogonGuid"`
	LogonID           string `json:"LogonId"`
	TerminalSessionID string `json:"TerminalSessionId"`
	IntegrityLevel    string `json:"IntegrityLevel"`
	Hashes            string `json:"Hashes"`
	ParentProcessGUID string `json:"ParentProcessGuid"`
	ParentProcessID   string `json:"ParentProcessId"`
	ParentImage       string `json:"ParentImage"`
	ParentCommandLine string `json:"ParentCommandLine"`
	EventReceivedTime string `json:"EventReceivedTime"`
	SourceModuleName  string `json:"SourceModuleName"`
	SourceModuleType  string `json:"SourceModuleType"`
}

func (s Sysmon) JSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s Sysmon) Source() Source {
	return Source{
		Host: s.Hostname,
		IP:   s.Host,
	}
}

func (s *Sysmon) Rename(pretty string) {
	s.Host = pretty
	s.Hostname = pretty
}

func (s Sysmon) Key() string {
	return s.Image
}

func (s Sysmon) GetEventTime() time.Time {
	return s.Timestamp
}
func (s Sysmon) GetSyslogTime() time.Time {
	return s.Timestamp
}
