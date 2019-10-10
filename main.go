package main

import (
	"context"
	"flag"
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"runtime"
	"time"
)

var (
	bootstrapServer = flag.String("bootstrap-server", "localhost:9092", "kafka cluster bootstrap servers")
	topic           = flag.String("topic", "", "kafka topic")
	fromBeginning   = flag.Bool("from-beginning", false, "start with the earliest message present")
	groupId         = flag.String("group", "test_consumer", "kafka consumer group")
	consumerThreads = flag.Int("consumer-threads", 1, "kafka consumer threads")
	consumerCount   = flag.Int("consumer-count", 1, "kafka consumer count")
	perfTestMode    = flag.Bool("perftest-mode", false, "Perf test mode without print message")
)

var messagesCounter uint64
var messagesSizeCounter uint64
var tl timeLag
var msgChan chan *kafka.Message
var consumersLags = newConsumersLagsMap()
var startTime time.Time
var kafkaConsumerConf *consumerConf

func main() {

	log.Println(fmt.Sprintf("Num CPU: %v", runtime.NumCPU()))
	log.Println(fmt.Sprintf("Prev GOMAXPROCS value: %v", runtime.GOMAXPROCS(runtime.NumCPU())))

	flag.Parse()
	kafkaConsumerConf = newConsumerConf(*fromBeginning, *bootstrapServer, *groupId, *topic, *consumerCount, *consumerThreads, *perfTestMode)
	msgChan = make(chan *kafka.Message, 10000)

	ctx, cancel := context.WithCancel(context.Background())
	go interruptListener(cancel)

	log.Println("start consuming")
	for i := 0; i < kafkaConsumerConf.consumerCount; i++ {
		go consumerWorker(ctx, kafkaConsumerConf)
	}
	startTime = time.Now()

	if kafkaConsumerConf.perfTestMode {
	loop1:
		for {
			select {
			case <-ctx.Done():
				break loop1
			default:
				continue
			}
		}

	} else {
	loop2:
		for {
			select {
			case <-ctx.Done():
				break loop2
			case msg := <-msgChan:
				log.Println(fmt.Sprintf("msg timestamp: %v, msg partition: %v, msg: %v", msg.Timestamp.String(), msg.TopicPartition, string(msg.Value)))
			default:
				continue
			}
		}
	}

	afterInterrupt()
}
