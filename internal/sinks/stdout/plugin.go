package stdout

import (
	"astro/internal/message"
	"astro/internal/schema"
	"astro/internal/sinks"
	"context"
	"fmt"
)

type SinkPlugin struct {
	ctx          context.Context
	streamSchema []schema.StreamSchema
	config       Config
}

func NewStdOutSinkPlugin(config Config, schema []schema.StreamSchema) sinks.DataSink {
	return &SinkPlugin{
		streamSchema: schema,
		config:       config,
	}
}

func (s *SinkPlugin) Connect(ctx context.Context) error {
	s.ctx = ctx
	return nil
}

func (s *SinkPlugin) Write(message message.Message) error {
	encodedMessage, _ := message.Data.MarshalJSON()
	fmt.Println("message from source", string(encodedMessage))

	return nil
}

func (s *SinkPlugin) GetType() sinks.SinkDriver {
	return sinks.StdOutSinkType
}

func (s *SinkPlugin) Stop() {
	s.ctx.Done()
}
