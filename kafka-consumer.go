package main

import (
	"flag"
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	bootstrapServer = flag.String("bootstrap-server", "localhost:9092", "kafka cluster bootstrap servers")
	topic           = flag.String("topic", "", "kafka topic")
	fromBeginning   = flag.Bool("from-beginning", false, "start with the earliest message present")
	groupId         = flag.String("group", "test_consumer", "kafka consumer group")
)

func main() {

	flag.Parse()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)

	var autoOffsetReset string
	if *fromBeginning {
		autoOffsetReset = "earliest"
	} else {
		autoOffsetReset = "latest"
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": *bootstrapServer,
		"group.id":          *groupId,
		"auto.offset.reset": autoOffsetReset,
	})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "invalid command option: %v", err.Error())
		flag.PrintDefaults()
		os.Exit(1)
	}

	err = c.SubscribeTopics([]string{*topic}, nil)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error subscribe topics: %v", err.Error())
		os.Exit(1)
	}
	defer c.Close()

	log.Println("start consuming")

	for {
		select {
		case <-sigc:
			c.Close()
			log.Println("stop consuming")
			os.Exit(0)
		default:
			msg, err := c.ReadMessage(-1)
			if err != nil {
				log.Println(fmt.Sprintf("kafka consumer: read from kafka error: %v", err))
				continue
			}

			log.Println(fmt.Sprintf("timestamp: %v, partition: %v, msg: %v", msg.Timestamp.String(), msg.TopicPartition, string(msg.Value)))
		}
	}
}
