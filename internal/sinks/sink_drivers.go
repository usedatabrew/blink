package sinks

type SinkDriver string

const (
	StdOutSinkType SinkDriver = "stdout"
	KafkaSinkType  SinkDriver = "kafka"
)
