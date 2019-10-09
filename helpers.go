package main

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
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

	consumersLags.Set(consumerId, n)
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
	allMessages := atomic.LoadUint64(&messagesCounter)
	allSize := ByteCountSI(int64(atomic.LoadUint64(&messagesSizeCounter)))
	rs := float64(allMessages) / time.Since(startTime).Seconds()
	log.Println(fmt.Sprintf("Comsumers lag: %v", consumersLags))
	log.Println(fmt.Sprintf("Read speed: %.2f msg/s, read time: %v", rs, time.Since(startTime)))
	log.Println(fmt.Sprintf("Number of messages received from kafka topic: %v", allMessages))
	log.Println(fmt.Sprintf("All size of messages received from kafka topic: %v", allSize))
	log.Println(fmt.Sprintf("Consumer idle number: %v", consumersLags.GetIdleNumber()))
	log.Println(fmt.Sprintf("Current time lag: %v", tl.GetCurrentTimeLag()))
	log.Println("stop consuming")
}
