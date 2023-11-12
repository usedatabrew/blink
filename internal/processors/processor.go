package processors

import (
	"context"
	"lunaflow/internal/message"
)

type DataProcessor interface {
	Process(context context.Context, message message.Message) error
}
