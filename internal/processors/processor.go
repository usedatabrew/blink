package processors

import (
	"astro/internal/message"
	"context"
)

type DataProcessor interface {
	Process(context context.Context, message message.Message) error
}
