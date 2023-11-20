package stdout

import (
	"astro/internal/message"
	"astro/internal/schema"
	"astro/internal/sinks"
	"astro/internal/stream_context"
	"context"
	"fmt"
	"github.com/charmbracelet/log"
	"os"
)

type SinkPlugin struct {
	appCtx       *stream_context.Context
	streamSchema []schema.StreamSchema
	config       Config
	logger       *log.Logger
	file         *os.File
}

func NewStdOutSinkPlugin(config Config, schema []schema.StreamSchema, appCtx *stream_context.Context) sinks.DataSink {
	file, err := os.OpenFile("example.data", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		fmt.Println("Could not open example.txt")
		panic(err)
	}

	return &SinkPlugin{
		streamSchema: schema,
		config:       config,
		file:         file,
		appCtx:       appCtx,
		logger:       appCtx.Logger.WithPrefix("[sink]: stdout"),
	}
}

func (s *SinkPlugin) Connect(ctx context.Context) error {
	return nil
}

func (s *SinkPlugin) Write(message message.Message) error {
	d, _ := message.Data.MarshalJSON()
	_, err := s.file.WriteString(string(d))
	return err
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
