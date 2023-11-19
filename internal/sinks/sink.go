package sinks

import (
	"astro/internal/message"
	"astro/internal/schema"
	"context"
)

type DataSink interface {
	Connect(context context.Context) error
	SetExpectedSchema(schema *schema.StreamSchemaObj)
	GetType() SinkDriver
	Write(m message.Message) error
	Stop()
}
