package rabbit_mq

import (
	"context"

	"github.com/charmbracelet/log"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sinks"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/message"

	"github.com/wagslane/go-rabbitmq"
)

type SinkPlugin struct {
	rabbitmqClient *rabbitmq.Conn
	publisher      *rabbitmq.Publisher
	streamSchema   []schema.StreamSchema
	config         Config
	logger         *log.Logger
}

func NewRabbitMqSinkPlugin(config Config, schema []schema.StreamSchema, appCtx *stream_context.Context) sinks.DataSink {
	return &SinkPlugin{
		streamSchema: schema,
		config:       config,
		logger:       appCtx.Logger.WithPrefix("[sink]: rabbit_mq"),
	}
}

func (s *SinkPlugin) Connect(ctx context.Context) error {
	client, err := rabbitmq.NewConn(s.config.Url, rabbitmq.WithConnectionOptionsLogging)

	if err != nil {
		return err
	}

	publisher, err := rabbitmq.NewPublisher(
		client,
		rabbitmq.WithPublisherOptionsLogging,
		rabbitmq.WithPublisherOptionsExchangeName(s.config.ExchangeName),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
	)

	if err != nil {
		return err
	}

	s.rabbitmqClient = client
	s.publisher = publisher
	return err
}

func (s *SinkPlugin) Write(message *message.Message) error {
	return s.publisher.Publish(
		[]byte(message.AsJSONString()),
		[]string{s.config.RoutingKey},
		rabbitmq.WithPublishOptionsContentType("application/json"),
		rabbitmq.WithPublishOptionsExchange(s.config.ExchangeName),
	)
}

func (s *SinkPlugin) GetType() sinks.SinkDriver {
	return sinks.RabbitMqSinkType
}

func (s *SinkPlugin) SetExpectedSchema(schema []schema.StreamSchema) {}

func (s *SinkPlugin) Stop() {
	s.publisher.Close()
	s.rabbitmqClient.Close()
}
