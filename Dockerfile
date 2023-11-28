FROM golang:1.21-bookworm

WORKDIR /app

COPY . .

RUN apt-get update && apt-get install -y \
    gcc \
    libc6-dev \
    librdkafka-dev
RUN go get gopkg.in/confluentinc/confluent-kafka-go.v1/kafka
RUN go mod tidy

RUN cd cmd/blink &&  go build blink/cmd/blink

ENV GOMAXPROCS=2
CMD ["./cmd/cmd"]