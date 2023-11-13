package stdout

import (
	"context"
	"fmt"
	"lunaflow/internal/message"
	"lunaflow/internal/sinks"
)

type SinkPlugin struct {
	ctx context.Context
}

func NewStdOutSinkPlugin() sinks.DataSink {
	return &SinkPlugin{}
}

func (s *SinkPlugin) Connect(ctx context.Context) error {
	s.ctx = ctx
	return nil
}

func (s *SinkPlugin) Write(message message.Message) {
	encodedMessage, _ := message.Data.MarshalJSON()
	fmt.Println("message from source", string(encodedMessage))
}

func (s *SinkPlugin) GetType() sinks.SinkType {
	return sinks.StdOutSinkType
}

func (s *SinkPlugin) Stop() {
	s.ctx.Done()
}
