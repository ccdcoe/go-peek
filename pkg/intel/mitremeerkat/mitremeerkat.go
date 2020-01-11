package mitremeerkat

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/go-redis/redis"
)

type Config struct {
	Host string
	Port int
	DB   int
}

func (c *Config) Validate() error {
	if c == nil {
		c = &Config{
			DB:   0,
			Port: 6379,
			Host: "localhost",
		}
	}
	if c.Host == "" {
		c.Host = "localhost"
	}
	if c.Port < 10 || c.Port > 65000 {
		c.Port = 6379
	}
	if c.DB < 0 {
		c.DB = 0
	}
	return nil
}

type RedisMap struct {
	ID     string `json:"id"`
	Sid    int    `json:"sid"`
	Tactic string `json:"tactic"`
	Name   string `json:"name"`
	Msg    string `json:"msg"`
}

type Mapper struct {
	handle  *redis.Client
	c       *Config
	results map[int]RedisMap
	missing map[int]bool
	hits    int
	mu      *sync.Mutex
}

func NewMapper(c *Config) (*Mapper, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	m := &Mapper{
		c:       c,
		results: make(map[int]RedisMap),
		missing: make(map[int]bool),
		mu:      &sync.Mutex{},
	}
	m.handle = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", m.c.Host, m.c.Port),
		Password: "",
		DB:       m.c.DB,
	})

	if _, err := m.handle.Ping().Result(); err != nil {
		return m, err
	}
	return m, nil
}

func (m *Mapper) GetSid(sid int) (RedisMap, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if val, ok := m.results[sid]; ok {
		m.hits++
		return val, ok
	}
	/*
		if m.missing[sid] == true {
			return RedisMap{}, false
		}
	*/
	raw, err := m.handle.Get(strconv.Itoa(sid)).Bytes()
	if err == redis.Nil {
		m.missing[sid] = true
		return RedisMap{}, false
	} else if err != nil {
		return RedisMap{}, false
	} else {
		var obj RedisMap
		if err := json.Unmarshal(raw, &obj); err != nil {
			return RedisMap{}, false
		}
		m.results[sid] = obj
		return obj, true
	}
}

func (m Mapper) Missing() []int {
	out := make([]int, 0)
	for _, obj := range m.results {
		out = append(out, obj.Sid)
	}
	return out
}
