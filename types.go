package main

import (
	"fmt"
	"sync"
	"time"
)

type timeLag struct {
	mx            sync.Mutex
	lastTimestamp time.Time
}

func (tl *timeLag) SetLastMessageTimestamp(t time.Time) {
	tl.mx.Lock()
	defer tl.mx.Unlock()
	tl.lastTimestamp = t
}

func (tl *timeLag) GetCurrentTimeLag() time.Duration {
	tl.mx.Lock()
	defer tl.mx.Unlock()
	return time.Since(tl.lastTimestamp)
}

type consumerConf struct {
	fromBeginning   bool
	bootstrapServer string
	groupId         string
	topic           string
	consumerCount   int
	consumerThreads int
	perfTestMode    bool
}

func newConsumerConf(fromBeginning bool, bootstrapServer string, groupId string, topic string, consumerCount int, consumerThreads int, perfTestMode bool) *consumerConf {
	return &consumerConf{
		fromBeginning:   fromBeginning,
		bootstrapServer: bootstrapServer,
		groupId:         groupId,
		topic:           topic,
		consumerCount:   consumerCount,
		consumerThreads: consumerThreads,
		perfTestMode:    perfTestMode,
	}
}

type consumersLagsMap struct {
	sync.RWMutex
	items map[string]string
}

func newConsumersLagsMap() *consumersLagsMap {
	return &consumersLagsMap{
		RWMutex: sync.RWMutex{},
		items:   make(map[string]string),
	}
}

func (cls *consumersLagsMap) Set(key string, val string) {
	cls.Lock()
	defer cls.Unlock()
	cls.items[key] = val
}

func (cls *consumersLagsMap) GetIdleNumber() int {
	cls.Lock()
	defer cls.Unlock()
	var counter int
	for _, v := range cls.items {
		if v == "none" {
			counter++
		}
	}
	return counter

}

func (cls *consumersLagsMap) String() string {
	cls.Lock()
	defer cls.Unlock()
	return fmt.Sprintf("%v", cls.items)
}
