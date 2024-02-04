package nats

import (
	"context"

	"github.com/charmbracelet/log"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sinks"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/message"

	"github.com/nats-io/nats.go"
)

type SinkPlugin struct {
	natsClient   *nats.Conn
	streamSchema []schema.StreamSchema
	config       Config
	logger       *log.Logger
}

func NewNatsSinkPlugin(config Config, schema []schema.StreamSchema, appCtx *stream_context.Context) sinks.DataSink {
	return &SinkPlugin{
		streamSchema: schema,
		config:       config,
		logger:       appCtx.Logger.WithPrefix("[sink]: nats"),
	}
}

func (s *SinkPlugin) Connect(ctx context.Context) error {
	client, err := nats.Connect(s.config.Url)

	if err != nil {
		return err
	}

	s.natsClient = client
	return err
}

func (s *SinkPlugin) Write(message *message.Message) error {
	return s.natsClient.Publish(s.config.Subject, []byte(message.AsJSONString()))
}

func (s *SinkPlugin) GetType() sinks.SinkDriver {
	return sinks.NatsSinkType
}

func (s *SinkPlugin) SetExpectedSchema(schema []schema.StreamSchema) {}

func (s *SinkPlugin) Stop() {
	s.natsClient.Close()
}
