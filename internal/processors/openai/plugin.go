package openai

import (
	"astro/internal/message"
	"astro/internal/schema"
	"astro/internal/stream_context"
	"context"
	"errors"
	"github.com/apache/arrow/go/v14/arrow"
)

type Plugin struct {
	config       Config
	resultSchema []schema.StreamSchema
}

func NewOpenAIPlugin(appctx stream_context.Context, config Config) (*Plugin, error) {
	return &Plugin{config: config}, nil
}

func (p *Plugin) Process(context context.Context, msg message.Message) (message.Message, error) {
	if msg.GetStream() != p.config.StreamName {
		return msg, nil
	}

	msg.SetNewField(p.config.TargetField, "ALLOW", arrow.BinaryTypes.String)

	return msg, nil
}

// MutateStreamSchema will add a string column to the schema in order to store OpenAI response
func (p *Plugin) MutateStreamSchema(streamSchema []schema.StreamSchema) ([]schema.StreamSchema, error) {
	var foundStream = false
	for _, stream := range streamSchema {
		if stream.StreamName == p.config.StreamName {
			foundStream = true
			arrowColumn := schema.Column{
				Name:                p.config.TargetField,
				DatabrewType:        "String",
				NativeConnectorType: "String",
				PK:                  false,
				Nullable:            true,
			}
			stream.Columns = append(stream.Columns, arrowColumn)

			break
		}
	}

	if !foundStream {
		return streamSchema, errors.New("stream not found. check configuration")
	}

	p.resultSchema = streamSchema
	return streamSchema, nil
}
