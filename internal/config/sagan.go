package config

type SaganConfig struct {
	// Kafka brokers list
	Brokers []string

	// Topic name for sagan, should be separate from original stream
	// Do not cross the streams
	Topic string
}

func NewSaganConfig() *SaganConfig {
	return &SaganConfig{
		Brokers: []string{"localhost:9092"},
	}
}

func NewDefaultSaganConfig(stream string) *SaganConfig {
	var s = NewSaganConfig()
	s.Topic = stream
	return s
}
