package app

import (
	"errors"
	"fmt"
	"go-peek/pkg/models/events"
	"strings"
)

type TopicMapFunc func(string) (events.Atomic, bool)

var ErrInvalidTopicFlags = errors.New("empty topic map")

type ErrInvalidTopicItem struct {
	Reason string
	Item   string
}

func (e ErrInvalidTopicItem) Error() string {
	return fmt.Sprintf("Invalid topic item: %s ; Reason: %s", e.Item, e.Reason)
}

func ParseKafkaTopicItems(flags []string) (KafkaTopicItems, error) {
	if flags == nil || len(flags) == 0 {
		return nil, ErrInvalidTopicFlags
	}
	items := make(KafkaTopicItems, len(flags))
	for i, item := range flags {
		bits := strings.Split(item, ":")
		if len(bits) != 2 {
			return items, ErrInvalidTopicItem{item, "should split to 2 substrings"}
		}
		if bits[0] == "" {
			return items, ErrInvalidTopicItem{item, "empty topic"}
		}
		if bits[1] == "" {
			return items, ErrInvalidTopicItem{item, "empty event type"}
		}
		eventType, ok := events.NewAtomic(bits[1])
		if !ok {
			return items, ErrInvalidTopicItem{item, "unknown event type " + bits[1]}
		}
		items[i] = KafkaTopicItem{
			Topic: bits[0],
			Type:  eventType,
		}
	}
	return items, nil
}

type KafkaTopicItem struct {
	Topic string
	Type  events.Atomic
}

type KafkaTopicItems []KafkaTopicItem

func (k KafkaTopicItems) Topics() []string {
	topics := make([]string, len(k))
	for i, item := range k {
		topics[i] = item.Topic
	}
	return topics
}

func (k KafkaTopicItems) TopicMap() TopicMapFunc {
	m := make(map[string]events.Atomic)
	for _, item := range k {
		m[item.Topic] = item.Type
	}
	return func(s string) (events.Atomic, bool) {
		if val, ok := m[s]; ok {
			return val, ok
		}
		return events.SimpleE, false
	}
}
