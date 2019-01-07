package config

type SaganConfig struct {
	// Enable sagan output for stream
	Enabled bool
}

func NewDefaultSaganConfig() *SaganConfig {
	return &SaganConfig{}
}
