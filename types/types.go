package types

import (
	"math/rand"
	"sync"
)

type BoolValues struct {
	*sync.Mutex
	m map[string]bool
}

func NewEmptyBoolValues() *BoolValues {
	return &BoolValues{
		Mutex: &sync.Mutex{},
		m:     map[string]bool{},
	}
}
func NewBoolValues(vals map[string]bool) *BoolValues {
	return &BoolValues{
		Mutex: &sync.Mutex{},
		m:     vals,
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
	return v.m[key]
}

func (v BoolValues) Len() int {
	return len(v.m)
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
	for k = range v.m {
		if i == 0 {
			break
		}
		i--
	}
	if remove {
		v.Del(k)
	} else {
		v.UnSet(k)
	}
	return k
}

func (v BoolValues) RawValues() map[string]bool {
	return v.m
}

type StringValues struct {
	*sync.Mutex
	m map[string]string
}

func NewStringValues() *StringValues {
	return &StringValues{
		Mutex: &sync.Mutex{},
		m:     map[string]string{},
	}
}

func (v *StringValues) Set(key, val string) *StringValues {
	v.Lock()
	defer v.Unlock()
	v.m[key] = val
	return v
}

func (v StringValues) Get(key string) string {
	return v.m[key]
}
