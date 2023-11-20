package kafka

import (
	"astro/internal/message"
	"astro/internal/schema"
	"astro/internal/sinks"
	"context"
	"github.com/charmbracelet/log"
	gokafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"strings"
)

type SinkPlugin struct {
	ctx          context.Context
	writer       *gokafka.Producer
	deliveryChan chan gokafka.Event
	writerConfig Config
	schema       []schema.StreamSchema
}

func NewKafkaSinkPlugin(config Config, schema []schema.StreamSchema) sinks.DataSink {
	plugin := &SinkPlugin{}
	plugin.writerConfig = config
	plugin.schema = schema
	plugin.deliveryChan = make(chan gokafka.Event, 400)
	plugin.ctx = context.Background()
	return plugin
}

func (s *SinkPlugin) Connect(ctx context.Context) error {
	p, err := gokafka.NewProducer(&gokafka.ConfigMap{
		"bootstrap.servers": strings.Join(s.writerConfig.Brokers, ","),
		"client.id":         "astro-writer",
		"acks":              "all",
		"security.protocol": "SASL_SSL",
		"go.batch.producer": true,
		"sasl.mechanisms":   s.writerConfig.SaslMechanism,
		"sasl.username":     s.writerConfig.SaslUser,
		"sasl.password":     s.writerConfig.SaslPassword,
	})

	if err != nil {
		return err
	}
	s.writer = p
	go s.readDeliveryChan()

	return nil
}

func (s *SinkPlugin) Write(mess message.Message) error {
	marshaledMessage, _ := mess.Data.MarshalJSON()

	err := s.writer.Produce(&gokafka.Message{
		TopicPartition: gokafka.TopicPartition{Topic: &s.writerConfig.TopicName, Partition: gokafka.PartitionAny},
		Key:            []byte(mess.GetEvent()),
		Value:          marshaledMessage,
	}, s.deliveryChan)

	if kafkaError, ok := err.(gokafka.Error); ok && kafkaError.Code() == gokafka.ErrQueueFull {
		log.Error("Kafka local queue full error - Going to Flush then retry...")
		flushedMessages := s.writer.Flush(10 * 1000)
		log.Infof("Flushed kafka messages. Outstanding events still un-flushed: %d", flushedMessages)
		return s.Write(mess)
	}

	return err
}

func (s *SinkPlugin) GetType() sinks.SinkDriver {
	return sinks.StdOutSinkType
}

func (s *SinkPlugin) SetExpectedSchema(schema []schema.StreamSchema) {

}

func (s *SinkPlugin) Stop() {
	s.writer.Close()
	s.ctx.Done()
}

func (s *SinkPlugin) readDeliveryChan() {
	for {
		select {
		case <-s.deliveryChan:

		}
	}
}
