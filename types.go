package main

import (
	"kafka-consumer/common"
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
	common.ConcurrentMap
}

func newConsumersLagsMap() *consumersLagsMap {
	return &consumersLagsMap{
		*common.NewConcurrentMap(),
	}
}

func (cls *consumersLagsMap) GetIdleNumber() int {
	var counter int
	for v := range cls.Iter() {
		if v.(string) == "none" {
			counter++
		}
	}
	return counter
}
