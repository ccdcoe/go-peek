package config

import (
	"time"

	"github.com/ccdcoe/go-peek/internal/outputs"
)

type StreamConfig struct {
	// Name of kafka input topic
	// Redundant if object is kept in map
	Name string

	// Type of messages in stream
	// To separate event.Event objects
	Type string

	// Output topic, elastic index name, etc
	// Timestamp should be appended to elastic index programmatically
	Topic string

	// Sagan is "Suricata but for logs"
	// Uses Suricata engine to correlate logs
	// Expects a specific log format
	// This config feeds messages back into backend/middleware kafka cluster in that format
	// So, new eventstream could be generated from correlations
	Sagan SaganConfig
}

func NewStreamConfig() *StreamConfig {
	return &StreamConfig{
		Sagan: SaganConfig{},
	}
}

func NewDefaultStreamConfig(stream string) *StreamConfig {
	var s = NewStreamConfig()
	s.Name = stream
	s.Type = stream
	s.Topic = stream
	s.Sagan = *NewDefaultSaganConfig()
	return s
}

func (s StreamConfig) ElaIdx(timestamp time.Time) string {
	return outputs.ElaIndex(s.Topic).Format(timestamp)
}
