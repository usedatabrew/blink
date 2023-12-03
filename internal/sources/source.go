package sources

import (
	"github.com/usedatabrew/blink/internal/message"
	"context"
)

type MessageEvent struct {
	Message message.Message
	Err     error
}

type DataSource interface {
	Connect(ctx context.Context) error
	Start()
	Events() chan MessageEvent
	Stop()
}
