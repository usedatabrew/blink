package stdout

import (
	"context"
	"fmt"
	"github.com/charmbracelet/log"
	"github.com/usedatabrew/blink/internal/message"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sinks"
	"github.com/usedatabrew/blink/internal/stream_context"
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
	d, _ := message.Data.MarshalJSON()
	s.logger.Info(string(d))
	return nil
}

func (s *SinkPlugin) GetType() sinks.SinkDriver {
	return sinks.StdOutSinkType
}

func (s *SinkPlugin) SetExpectedSchema(schema []schema.StreamSchema) {
	fmt.Println("Expected sink schema", schema)
}

func (s *SinkPlugin) Stop() {
	s.appCtx.GetContext().Done()
}
