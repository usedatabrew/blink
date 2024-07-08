package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sinks"
	"github.com/usedatabrew/message"
)

type SinkPlugin struct {
	ctx          context.Context
	writer       *kgo.Client
	writerConfig Config
	schema       []schema.StreamSchema

	messageBatch      []*message.Message
	maxBatchSize      int
	batchTickInterval time.Duration
	mu                sync.Mutex

	done chan struct{}
}

func NewKafkaSinkPlugin(config Config, schema []schema.StreamSchema) sinks.DataSink {
	plugin := &SinkPlugin{}
	plugin.writerConfig = config
	plugin.schema = schema
	plugin.maxBatchSize = 10000
	plugin.messageBatch = []*message.Message{}
	plugin.batchTickInterval = time.Millisecond * 400
	plugin.done = make(chan struct{})
	plugin.ctx = context.Background()
	return plugin
}

func (s *SinkPlugin) Connect(ctx context.Context) error {
	go s.startFlushRoutine()

	options := []kgo.Opt{
		kgo.AllowAutoTopicCreation(),
	}

	options = append(options, s.GetConfig()...)

	client, err := kgo.NewClient(options...)

	if err != nil {
		panic(err)
	}

	err = client.Ping(context.Background())

	if err != nil {
		return err
	}

	admin := kadm.NewClient(client)
	defer admin.Close()

	_, err = admin.CreateTopics(s.ctx, 1, -1, nil, s.writerConfig.TopicName)
	if err != nil {
		panic(err)
	}

	s.writer = client

	return nil
}

func (s *SinkPlugin) Write(mess *message.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.messageBatch = append(s.messageBatch, mess)

	// Check if the buffer size has reached the threshold
	if len(s.messageBatch) >= s.maxBatchSize {
		return s.flushBuffer()
	}

	return nil
}

func (s *SinkPlugin) startFlushRoutine() {
	ticker := time.NewTicker(s.batchTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			if err := s.flushBuffer(); err != nil {
				panic(err)
			}
			s.mu.Unlock()
		case <-s.done:
			return
		}
	}
}

func (s *SinkPlugin) flushBuffer() error {
	if len(s.messageBatch) > 0 {
		// Replace this with your actual processing logic
		var records []*kgo.Record
		for _, mess := range s.messageBatch {
			marshaledMessage := []byte(mess.AsJSONString())
			topic := s.writerConfig.TopicName
			if s.writerConfig.BindTopicToStream {
				topic = mess.GetStream()
			}
			record := &kgo.Record{Topic: topic, Value: marshaledMessage}
			records = append(records, record)
		}

		if err := s.writer.ProduceSync(context.Background(), records...).FirstErr(); err != nil {
			fmt.Printf("record had a produce error while synchronously producing: %v\n", err)
			return err
		}

		// Clear the buffer
		s.messageBatch = []*message.Message{}
	}

	return nil
}

func (s *SinkPlugin) GetType() sinks.SinkDriver {
	return sinks.KafkaSinkType
}

func (s *SinkPlugin) SetExpectedSchema(schema []schema.StreamSchema) {

}

func (s *SinkPlugin) GetConfig() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.DialTLSConfig(new(tls.Config)),
		kgo.SeedBrokers(s.writerConfig.Brokers...),
		kgo.ProducerBatchMaxBytes(int32(6000000)),
	}

	if s.writerConfig.Sasl {
		if s.writerConfig.SaslMechanism == "" || s.writerConfig.SaslUser == "" || s.writerConfig.SaslPassword == "" {
			panic("sasl param must be specified")
		}
		method := strings.ToLower(s.writerConfig.SaslMechanism)
		method = strings.ReplaceAll(method, "-", "")
		method = strings.ReplaceAll(method, "_", "")
		switch method {
		case "plain":
			opts = append(opts, kgo.SASL(plain.Auth{
				User: s.writerConfig.SaslUser,
				Pass: s.writerConfig.SaslPassword,
			}.AsMechanism()))
		case "scramsha256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: s.writerConfig.SaslUser,
				Pass: s.writerConfig.SaslPassword,
			}.AsSha256Mechanism()))
		case "scramsha512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: s.writerConfig.SaslUser,
				Pass: s.writerConfig.SaslPassword,
			}.AsSha512Mechanism()))
		case "awsmskiam":
			opts = append(opts, kgo.SASL(aws.Auth{
				AccessKey: s.writerConfig.SaslUser,
				SecretKey: s.writerConfig.SaslPassword,
			}.AsManagedStreamingIAMMechanism()))
		default:
			panic(fmt.Sprintf("unrecognized sasl option %s", s.writerConfig.SaslMechanism))
		}
	}

	return opts
}

func (s *SinkPlugin) Stop() {
	close(s.done)
	s.writer.Close()
	s.ctx.Done()
}
