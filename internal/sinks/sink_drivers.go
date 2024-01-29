package sinks

type SinkDriver string

const (
	StdOutSinkType    SinkDriver = "stdout"
	WebSocketSinkType SinkDriver = "websocket"
	KafkaSinkType     SinkDriver = "kafka"
	PostgresSinkType  SinkDriver = "postgres"
	MongoDBSinkType   SinkDriver = "mongodb"
	NatsSinkType      SinkDriver = "nats"
	RabbitMqSinkType  SinkDriver = "rabbitmq"
)
