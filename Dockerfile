FROM ubuntu:16.04

WORKDIR /go/src/kafka-consumer

ADD kafka-consumer.go kafka-consumer.go

ENV gopkg go1.13.1.linux-amd64.tar.gz
ENV goroot /usr/local/go
ENV gopath /go

RUN apt-get update && apt-get install -y wget git pkg-config \
    && wget -qO - https://packages.confluent.io/deb/5.3/archive.key | apt-key add - \
    && echo "deb [arch=amd64] https://packages.confluent.io/deb/5.3 stable main" >> /etc/apt/sources.list \
    && apt-get update && apt-get install -y librdkafka1 librdkafka++1 librdkafka-dev \
    && wget https://dl.google.com/go/${gopkg} \
    && tar -xvf ${gopkg} && mv go /usr/local \
    && rm -rf ./${gopkg} \
    && export GOROOT=${goroot} && export GOPATH=${gopath} \
    && export PATH=${gopath}/bin:${goroot}/bin:$PATH && go version \
    && go get -u gopkg.in/confluentinc/confluent-kafka-go.v1/kafka \
    && go build kafka-consumer.go \
    && chmod +x kafka-consumer
