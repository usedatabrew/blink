package sinks

import (
	"context"
	"lunaflow/internal/message"
)

type DataSink interface {
	Connect(context context.Context) error
	GetType() SinkType
	Write(m message.Message)
	Stop()
}
