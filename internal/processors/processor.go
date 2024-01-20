package processors

import (
	"context"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/message"
)

type ProcessorDriver string

const (
	OpenAIProcessor             ProcessorDriver = "openai"
	AIContentModeratorProcessor ProcessorDriver = "ai_content_moderator"
	SQLProcessor                ProcessorDriver = "sql"
	LambdaProcessor             ProcessorDriver = "lambda"
	HttpProcessor               ProcessorDriver = "http"
	SQLEnrichProcessor          ProcessorDriver = "sql_enrich"
	LogProcessor                ProcessorDriver = "log"
)

type DataProcessor interface {
	// Process accepts the message from the pipeline and changes it
	// before returning and passing to the downstream.
	// !Important! Try to not copy the message inside a pipeline to avoid redundant
	// memory allocations and make overall performance worth :(
	Process(context context.Context, message *message.Message) (*message.Message, error)
	// EvolveSchema is being called before starting the pipeline
	// to form the schema that will be used as a result of the processing
	// If your processor doesn't add/delete any fields for the message
	// you can simply return the schema you received as an argument
	EvolveSchema(schema *schema.StreamSchemaObj) error
}
