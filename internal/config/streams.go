package config

import (
	"fmt"
	"time"

	"github.com/ccdcoe/go-peek/internal/outputs"
)

type Streams map[string]*StreamConfig

func (s Streams) EventTypes() map[string]string {
	var types = make(map[string]string)
	for k, v := range s {
		types[k] = v.Type
	}
	return types
}

func (s Streams) GetType(key string) (string, bool) {
	if val, ok := s[key]; ok {
		return val.Type, true
	}
	return "", false
}
func (s Streams) GetTopic(key string) (string, bool) {
	if val, ok := s[key]; ok {
		return val.Topic, true
	}
	return "", false
}
func (s Streams) GetSaganTopic(key string) (string, bool) {
	if val, ok := s[key]; ok {
		if val.Sagan.Topic == "" {
			return fmt.Sprintf("%s-sagan", val.Topic), false
		}
		return val.Sagan.Topic, true
	}
	return "", false
}

func (s Streams) Topics() []string {
	var topics = make([]string, 0)
	for k := range s {
		topics = append(topics, k)
	}
	return topics
}

func (s Streams) ElaIdx(timestamp time.Time, event string) string {
	if val, ok := s[event]; ok {
		return val.ElaIdx(timestamp)
	}
	return outputs.ElaIndex("events").Format(timestamp)
}
