package sinks

type SinkType string

const (
	StdOutSinkType SinkType = "stdout"
	KafkaSinkType  SinkType = "kafka"
)
