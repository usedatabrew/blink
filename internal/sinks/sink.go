package sinks

import (
	"context"
	"github.com/usedatabrew/blink/internal/message"
	"github.com/usedatabrew/blink/internal/schema"
)

type DataSink interface {
	Connect(context context.Context) error
	SetExpectedSchema(schema []schema.StreamSchema)
	GetType() SinkDriver
	Write(m *message.Message) error
	Stop()
}
