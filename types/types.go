package types

import (
	"math/rand"
	"sync"
)

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
	v.RLock()
	defer v.RUnlock()
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
	v.RLock()
	for k = range vals {
		if i == 0 {
			break
		}
		i--
	}
	v.RUnlock()

	if remove {
		v.Del(k)
	} else {
		v.UnSet(k)
	}
	return k
}

func (v BoolValues) RawValues() map[string]bool {
	v.RLock()
	defer v.RUnlock()
	return v.m
}

type StringValues struct {
	*sync.RWMutex
	m map[string]string
}

func NewStringValues() *StringValues {
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
	v.RLock()
	defer v.RUnlock()
	if val, ok := v.m[key]; ok {
		return val, true
	}
	return "", false
}
