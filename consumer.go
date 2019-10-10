package main

import (
	"context"
	"flag"
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"os"
	"runtime"
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
		//https://gist.github.com/edenhill/9dfc019b980a5eb3365c84524a1f12b0
		"go.application.rebalance.enable": true,
		"enable.partition.eof":            true,
		"enable.auto.commit":              false,
		"session.timeout.ms":              6000,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"},
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

	//if cc.fromBeginning {
	//	kafkaConfigMap.Set("auto.offset.reset=earliest")
	//}

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

func consumerWorker(ctx context.Context, cc *consumerConf) {

	c := newConsumer(cc)
	defer c.Close()

	consumersLags.Set(c.String(), "none")

	//var waitGroup sync.WaitGroup

Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		default:
			// +default
			//for i := 0; i < cc.consumerThreads; i++ {
			//waitGroup.Add(1)
			//go func(wg *sync.WaitGroup) {

			// +workerBody
			//defer wg.Done()
			//msg, err := c.ReadMessage(-1)
			//===============

			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case kafka.AssignedPartitions:

				log.Println("Rebalance evenent:")

				ap, _ := c.Assignment()
				log.Printf("Assigment partitions: %v", ap)
				co, _ := c.Committed(ap, 2000)
				log.Printf("Committed offsets: %v", co)

				parts := make([]kafka.TopicPartition, len(e.Partitions))
				for i, tp := range e.Partitions {
					tp.Offset = kafka.OffsetTail(5)
					parts[i] = tp
				}
				log.Printf("Assign %v\n", parts)
				c.Assign(parts)

				ap, _ = c.Assignment()
				log.Printf("Assigment partitions: %v", ap)
				co, _ = c.Committed(ap, 2000)
				log.Printf("Committed offsets: %v", co)

			case kafka.RevokedPartitions:
				log.Printf("Revoked partitions event: %v\n", e)
			case *kafka.Message:
				log.Println("Read message event:")
				atomic.AddUint64(&messagesCounter, 1)
				atomic.AddUint64(&messagesSizeCounter, uint64(len(e.Value)))
				go tl.SetLastMessageTimestamp(e.Timestamp)
				if !cc.perfTestMode {
					msgChan <- e
				}
				tp, err := c.CommitMessage(e)
				if err != nil {
					fmt.Println(err)
				}
				fmt.Printf("Commit %v \n", tp)
				runtime.Gosched()
			case kafka.PartitionEOF:
				log.Printf("Partition EOF event: %v\n", e)
			case kafka.OffsetsCommitted:
				log.Printf("Offsets committed event: %v\n", e.Offsets)
			case kafka.Error:
				log.Printf("Error event: %v\n", e)
			default:
				log.Printf("Unhandled event: %v\n", e)
			}

			//===============
			//if err != nil {
			//	runtime.Gosched()
			//	return
			//}
			//atomic.AddUint64(&messagesCounter, 1)
			//atomic.AddUint64(&messagesSizeCounter, uint64(len(msg.Value)))
			//go tl.SetLastMessageTimestamp(msg.Timestamp)
			//if !cc.perfTestMode {
			//	msgChan <- msg
			//}
			//runtime.Gosched()
			// -workerBody

			//}(&waitGroup)
			//}
			//waitGroup.Wait()
			go setConsumerLag(c, c.String())
			runtime.Gosched()
			// -default
		}
	}
}
