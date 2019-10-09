FROM ubuntu:16.04

WORKDIR /go/src/kafka-consumer

ADD kafka-consumer.go kafka-consumer.go

ENV GOPKG go1.13.1.linux-amd64.tar.gz
ENV GOROOT /usr/local/go
ENV GOPATH /go

RUN apt-get update && apt-get install -y wget git pkg-config apt-transport-https ca-certificates \
    && wget -qO - https://packages.confluent.io/deb/5.3/archive.key | apt-key add - \
    && echo "deb [arch=amd64] https://packages.confluent.io/deb/5.3 stable main" >> /etc/apt/sources.list \
    && apt-get update && apt-get install -y librdkafka1 librdkafka++1 librdkafka-dev \
    && wget https://dl.google.com/go/${GOPKG} \
    && tar -xvf ${GOPKG} && mv go /usr/local \
    && rm -rf ./${GOPKG} \
    && export GOROOT=${GOROOT} && export GOPATH=${GOPATH} \
    && export PATH=${GOPATH}/bin:${GOROOT}/bin:$PATH && go version \
    && go get -u gopkg.in/confluentinc/confluent-kafka-go.v1/kafka \
    && go build kafka-consumer.go \
    && chmod +x kafka-consumer
