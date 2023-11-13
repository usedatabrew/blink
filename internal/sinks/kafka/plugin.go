package kafka

import (
	"context"
	"fmt"
	gokafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"lunaflow/internal/message"
	"lunaflow/internal/sinks"
	"strings"
)

type SinkPlugin struct {
	ctx    context.Context
	writer *gokafka.Producer

	writerConfig Config
}

func NewKafkaSinkPlugin(config Config) sinks.DataSink {
	plugin := &SinkPlugin{}
	plugin.writerConfig = config
	plugin.ctx = context.Background()
	return plugin
}

func (s *SinkPlugin) Connect(ctx context.Context) error {
	p, err := gokafka.NewProducer(&gokafka.ConfigMap{
		"bootstrap.servers": strings.Join(s.writerConfig.Brokers, ","),
		"client.id":         "lunaflow-writer",
		"acks":              "all",
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   s.writerConfig.SaslMechanism,
		"sasl.username":     s.writerConfig.SaslUser,
		"sasl.password":     s.writerConfig.SaslPassword,
	})

	if err != nil {
		return err
	}
	s.writer = p

	return nil
}

func (s *SinkPlugin) Write(mess message.Message) {
	deliveryChan := make(chan gokafka.Event, 500)
	marshaledMessage, _ := mess.Data.MarshalJSON()
	err := s.writer.Produce(&gokafka.Message{
		TopicPartition: gokafka.TopicPartition{Topic: &s.writerConfig.TopicName, Partition: gokafka.PartitionAny},
		Value:          marshaledMessage},
		deliveryChan,
	)
	if err != nil {
		fmt.Println("error message", err)
	}
}

func (s *SinkPlugin) GetType() sinks.SinkType {
	return sinks.StdOutSinkType
}

func (s *SinkPlugin) Stop() {
	s.writer.Close()
	s.ctx.Done()
}
