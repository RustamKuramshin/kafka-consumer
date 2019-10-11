# kafka-consumer

#### Многопоточный потребитель Apache Kafka с метриками производительности.

`kafka-consumer --help` \
Usage of kafka-consumer: \
**-bootstrap-server** *string* \
    	kafka cluster bootstrap servers (default "localhost:9092") \
**-consumer-count** *int* \
    	kafka consumer count (default 1) \
**-consumer-threads** *int* \
    	kafka consumer threads (default 1) \
**-from-beginning** \
    	start with the earliest message present \
**-group** *string* \
    	kafka consumer group (default "test_consumer") \
**-perftest-mode** \
    	Perf test mode without print message \
**-topic** string \
    	kafka topic
