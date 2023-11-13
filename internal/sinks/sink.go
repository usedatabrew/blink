package sinks

import (
	"astro/internal/message"
	"context"
)

type DataSink interface {
	Connect(context context.Context) error
	GetType() SinkDriver
	Write(m message.Message) error
	Stop()
}
