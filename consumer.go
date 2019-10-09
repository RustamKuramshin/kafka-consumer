package main

import (
	"context"
	"flag"
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
)

func newConsumer(cc *consumerConf) *kafka.Consumer {

	var kafkaConfigMap = &kafka.ConfigMap{
		"bootstrap.servers":       cc.bootstrapServer,
		"group.id":                cc.groupId,
		"queued.min.messages":     10000,
		"fetch.message.max.bytes": 32000,
		"fetch.wait.max.ms":       1000,
		"fetch.error.backoff.ms":  200,
	}
	//var kafkaConfigMap = &kafka.ConfigMap{
	//	"bootstrap.servers": cc.bootstrapServer,
	//	"group.id":          cc.groupId,
	//	"queue.buffering.max.ms":100,
	//	"go.events.channel.size":100000,
	//	"go.application.rebalance.enable":true,
	//	"fetch.min.bytes":1,
	//	"max.in.flight.requests.per.connection":100000,
	//	"queue.buffering.max.messages":1000,
	//	"enable.auto.commit":false,
	//	"session.timeout.ms":10000,
	//	"enable.partition.eof":false,
	//	"enable.auto.offset.store":false,
	//	"batch.num.messages":10000,
	//	"go.events.channel.enable":true,
	//	"compression.codec":"snappy",
	//	"fetch.message.max.bytes":32768,
	//	"fetch.wait.max.ms":1000,
	//}
	if cc.fromBeginning {
		kafkaConfigMap.Set("auto.offset.reset=earliest")
	}

	c, err := kafka.NewConsumer(kafkaConfigMap)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "invalid command option: %v", err.Error())
		flag.PrintDefaults()
		os.Exit(1)
	}
	err = c.SubscribeTopics([]string{cc.topic}, nil)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error subscribe topics: %v", err.Error())
		os.Exit(1)
	}

	return c
}

func consumerWorker(ctx context.Context, id string, cc *consumerConf) {

	c := newConsumer(cc)
	defer c.Close()

	consumersLags.Set(id, -1)

	var waitGroup sync.WaitGroup

Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		default:
			// +default
			for i := 0; i < cc.consumerThreads; i++ {
				waitGroup.Add(1)
				go func(wg *sync.WaitGroup) {

					// +workerBody
					defer wg.Done()
					msg, err := c.ReadMessage(-1)
					if err != nil {
						runtime.Gosched()
						return
					}
					atomic.AddUint64(&messagesCounter, 1)
					atomic.AddUint64(&messagesSizeCounter, uint64(len(msg.Value)))
					go tl.SetLastMessageTimestamp(msg.Timestamp)
					if !cc.perfTestMode {
						msgChan <- msg
					}
					runtime.Gosched()
					// -workerBody

				}(&waitGroup)

			}
			waitGroup.Wait()
			go setConsumerLag(c, id)
			runtime.Gosched()
			// -default
		}
	}
}
