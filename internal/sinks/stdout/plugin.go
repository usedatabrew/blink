package stdout

import (
	"astro/internal/message"
	"astro/internal/schema"
	"astro/internal/sinks"
	"astro/internal/stream_context"
	"context"
	"github.com/charmbracelet/log"
)

type SinkPlugin struct {
	appCtx       *stream_context.Context
	streamSchema []schema.StreamSchema
	config       Config
	logger       *log.Logger
}

func NewStdOutSinkPlugin(config Config, schema []schema.StreamSchema, appCtx *stream_context.Context) sinks.DataSink {
	return &SinkPlugin{
		streamSchema: schema,
		config:       config,
		appCtx:       appCtx,
		logger:       appCtx.Logger.WithPrefix("[sink]: stdout"),
	}
}

func (s *SinkPlugin) Connect(ctx context.Context) error {
	return nil
}

func (s *SinkPlugin) Write(message message.Message) error {
	encodedMessage, _ := message.Data.MarshalJSON()
	s.logger.Info("Message from source received", string(encodedMessage))

	return nil
}

func (s *SinkPlugin) GetType() sinks.SinkDriver {
	return sinks.StdOutSinkType
}

func (s *SinkPlugin) Stop() {
	s.appCtx.GetContext().Done()
}
