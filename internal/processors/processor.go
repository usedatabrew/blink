package processors

import (
	"astro/internal/message"
	"astro/internal/schema"
	"context"
)

type DataProcessor interface {
	Process(context context.Context, message message.Message) error
	MutateStreamSchema(schema []schema.StreamSchema) []schema.StreamSchema
}
