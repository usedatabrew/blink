package sinks

import (
	"blink/internal/message"
	"blink/internal/schema"
	"context"
)

type DataSink interface {
	Connect(context context.Context) error
	SetExpectedSchema(schema []schema.StreamSchema)
	GetType() SinkDriver
	Write(m message.Message) error
	Stop()
}
