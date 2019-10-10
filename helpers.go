package main

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"strconv"
	"sync/atomic"
	"time"
)

func setConsumerLag(cnsmr *kafka.Consumer, consumerId string) {

	var n int

	topPars, err := cnsmr.Assignment()
	if err != nil {
		log.Println(err)
		return
	}

	topPars, err = cnsmr.Committed(topPars, 5000)
	if err != nil {
		log.Println(err)
		return
	}

	var l, h int64
	for i := range topPars {
		l, h, err = cnsmr.QueryWatermarkOffsets(*topPars[i].Topic, topPars[i].Partition, 5000)
		if err != nil {
			log.Println(err)
			return
		}

		o := int64(topPars[i].Offset)
		if topPars[i].Offset == kafka.OffsetInvalid {
			o = l
		}

		n = n + int(h-o)
	}

	consumersLags.Set(consumerId, strconv.Itoa(n))
}

func ByteCountSI(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}

func afterInterrupt() {
	allMessages := float64(atomic.LoadUint64(&messagesCounter))
	allMessagesSizeInByte := int64(atomic.LoadUint64(&messagesSizeCounter))
	allMessagesSizeFormatted := ByteCountSI(allMessagesSizeInByte)

	messagesRate := allMessages / time.Since(startTime).Seconds()
	dataRate := ByteCountSI(int64(float64(allMessagesSizeInByte) / time.Since(startTime).Seconds()))

	fmt.Println("==================================STAT=================================================")

	log.Println(fmt.Sprintf("Number of messages received from kafka topic: %v", allMessages))
	log.Println(fmt.Sprintf("All size of messages received from kafka topic: %v", allMessagesSizeFormatted))

	log.Println(fmt.Sprintf("Messages rate: %.2f msg/s, data rate: %v/s, all time: %v", messagesRate, dataRate, time.Since(startTime)))

	log.Println(fmt.Sprintf("Comsumers lag: %v", consumersLags))
	log.Println(fmt.Sprintf("Current time lag (TimeNow - LastTimestamp): %v", tl.GetCurrentTimeLag()))

	log.Println(fmt.Sprintf("Consumer count: %v", kafkaConsumerConf.consumerCount))
	log.Println(fmt.Sprintf("Consumers idle: %v", consumersLags.GetIdleNumber()))
	log.Println(fmt.Sprintf("Threads per consumer: %v", kafkaConsumerConf.consumerThreads))

	fmt.Println("==================================STAT=================================================")
	log.Println("stop consuming")
}
