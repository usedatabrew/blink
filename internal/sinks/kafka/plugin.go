package kafka

import (
	"astro/internal/message"
	"astro/internal/schema"
	"astro/internal/sinks"
	"context"
	gokafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"strings"
)

type SinkPlugin struct {
	ctx    context.Context
	writer *gokafka.Producer

	writerConfig Config
	schema       []schema.StreamSchema
}

func NewKafkaSinkPlugin(config Config, schema []schema.StreamSchema) sinks.DataSink {
	plugin := &SinkPlugin{}
	plugin.writerConfig = config
	plugin.schema = schema
	plugin.ctx = context.Background()
	return plugin
}

func (s *SinkPlugin) Connect(ctx context.Context) error {
	p, err := gokafka.NewProducer(&gokafka.ConfigMap{
		"bootstrap.servers": strings.Join(s.writerConfig.Brokers, ","),
		"client.id":         "astro-writer",
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

func (s *SinkPlugin) Write(mess message.Message) error {
	deliveryChan := make(chan gokafka.Event, 300)
	marshaledMessage, _ := mess.Data.MarshalJSON()
	err := s.writer.Produce(&gokafka.Message{
		TopicPartition: gokafka.TopicPartition{Topic: &s.writerConfig.TopicName, Partition: gokafka.PartitionAny},
		Value:          marshaledMessage},
		deliveryChan,
	)

	if err != nil {
		return err
	}

	return err
}

func (s *SinkPlugin) GetType() sinks.SinkDriver {
	return sinks.StdOutSinkType
}

func (s *SinkPlugin) Stop() {
	s.writer.Close()
	s.ctx.Done()
}
