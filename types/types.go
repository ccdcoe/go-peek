package types

import (
	"math/rand"
	"net"
	"strconv"
	"sync"
)

type StringIP struct{ net.IP }

func (t *StringIP) UnmarshalJSON(b []byte) error {
	raw, err := strconv.Unquote(string(b))
	if err != nil {
		return err
	}
	t.IP = net.ParseIP(raw)
	return err
}

type BoolValues struct {
	*sync.RWMutex
	m map[string]bool
}

func NewEmptyBoolValues() *BoolValues {
	return &BoolValues{
		RWMutex: &sync.RWMutex{},
		m:       map[string]bool{},
	}
}
func NewBoolValues(vals map[string]bool) *BoolValues {
	return &BoolValues{
		RWMutex: &sync.RWMutex{},
		m:       vals,
	}
}

func (v *BoolValues) Set(key string) *BoolValues {
	v.Lock()
	defer v.Unlock()
	v.m[key] = true
	return v
}

func (v *BoolValues) UnSet(key string) *BoolValues {
	v.Lock()
	defer v.Unlock()
	v.m[key] = false
	return v
}

func (v BoolValues) Get(key string) bool {
	v.Lock()
	defer v.Unlock()
	return v.m[key]
}

func (v BoolValues) Len() int {
	v.Lock()
	defer v.Unlock()
	l := len(v.m)
	return l
}

func (v *BoolValues) Del(key string) *BoolValues {
	v.Lock()
	defer v.Unlock()
	delete(v.m, key)
	return v
}

func (v BoolValues) GetRandom(remove bool) string {
	i := rand.Intn(v.Len())
	var k string

	vals := v.RawValues()
	v.Lock()
	for k = range vals {
		if i == 0 {
			break
		}
		i--
	}
	v.Unlock()

	if remove {
		v.Del(k)
	} else {
		v.UnSet(k)
	}
	return k
}

func (v BoolValues) RawValues() map[string]bool {
	v.Lock()
	defer v.Unlock()
	return v.m
}

type StringValues struct {
	*sync.RWMutex
	m map[string]string
}

func NewStringValues(vals map[string]string) *StringValues {
	return &StringValues{
		RWMutex: &sync.RWMutex{},
		m:       vals,
	}
}
func NewEmptyStringValues() *StringValues {
	return &StringValues{
		RWMutex: &sync.RWMutex{},
		m:       map[string]string{},
	}
}

func (v *StringValues) Set(key, val string) *StringValues {
	v.Lock()
	defer v.Unlock()
	v.m[key] = val
	return v
}

func (v StringValues) Get(key string) (string, bool) {
	v.Lock()
	defer v.Unlock()
	if val, ok := v.m[key]; ok {
		return val, true
	}
	return "", false
}

func (v StringValues) RawValues() map[string]string {
	v.Lock()
	defer v.Unlock()
	vals := map[string]string{}
	for k, val := range v.m {
		vals[k] = val
	}
	return vals
}
