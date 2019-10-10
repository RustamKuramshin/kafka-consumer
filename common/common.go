package common

import (
	"fmt"
	"sync"
)

type ConcurrentMap struct {
	mx    sync.Mutex
	items map[string]interface{}
}

func NewConcurrentMap() *ConcurrentMap {
	return &ConcurrentMap{
		items: make(map[string]interface{}),
	}
}

func (cm *ConcurrentMap) Get(key string) (interface{}, bool) {
	cm.mx.Lock()
	defer cm.mx.Unlock()
	val, ok := cm.items[key]
	return val, ok
}

func (cm *ConcurrentMap) Add(key string, val interface{}) {
	cm.mx.Lock()
	defer cm.mx.Unlock()
	cm.items[key] = val
}

func (cm *ConcurrentMap) Delete(key string) {
	cm.mx.Lock()
	defer cm.mx.Unlock()
	delete(cm.items, key)
}

func (cm *ConcurrentMap) Iter() chan interface{} {

	c := make(chan interface{})

	go func() {
		cm.mx.Lock()
		defer cm.mx.Unlock()
		for _, v := range cm.items {
			c <- v
		}
		close(c)
	}()

	return c
}

func (cm *ConcurrentMap) String() string {
	cm.mx.Lock()
	defer cm.mx.Unlock()
	return fmt.Sprintf("%v", cm.items)
}
