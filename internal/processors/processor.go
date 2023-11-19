package processors

import (
	"astro/internal/message"
	"astro/internal/schema"
	"context"
)

type ProcessorType string

const (
	OpenAIProcessor    ProcessorType = "openai"
	LambdaProcessor    ProcessorType = "lambda"
	HttpGetProcessor   ProcessorType = "http_get"
	SQLEnrichProcessor ProcessorType = "sql_enrich"
)

type DataProcessor interface {
	// Process accepts the message from the pipeline and changes it
	// before returning and passing to the downstream.
	// !Important! Try to not copy the message inside a pipeline to avoid redundant
	// memory allocations and make overall performance worth :(
	Process(context context.Context, message message.Message) (message.Message, error)
	// MutateStreamSchema is being called before starting the pipeline
	// to form the schema that will be used as a result of the processing
	// If your processor doesn't add/delete any fields for the message
	// you can simply return the schema you received as an argument
	MutateStreamSchema(schema []schema.StreamSchema) ([]schema.StreamSchema, error)
}
