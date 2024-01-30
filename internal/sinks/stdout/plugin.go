package stdout

import (
	"context"

	"github.com/charmbracelet/log"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sinks"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/message"
)

type SinkPlugin struct {
	streamSchema []schema.StreamSchema
	config       Config
	logger       *log.Logger
}

func NewStdOutSinkPlugin(config Config, schema []schema.StreamSchema, appCtx *stream_context.Context) sinks.DataSink {
	return &SinkPlugin{
		streamSchema: schema,
		config:       config,
		logger:       appCtx.Logger.WithPrefix("[sink]: stdout"),
	}
}

func (s *SinkPlugin) Connect(ctx context.Context) error {
	return nil
}

func (s *SinkPlugin) Write(message *message.Message) error {
	d := message.AsJSONString()
	s.logger.Info(d)
	return nil
}

func (s *SinkPlugin) GetType() sinks.SinkDriver {
	return sinks.StdOutSinkType
}

// SetExpectedSchema for Stdout component does nothing, since this component is used mostly for debugging
func (s *SinkPlugin) SetExpectedSchema(schema []schema.StreamSchema) {
}

func (s *SinkPlugin) Stop() {}
