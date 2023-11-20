package openai

import (
	"astro/internal/message"
	"astro/internal/schema"
	"astro/internal/stream_context"
	"context"
	"github.com/apache/arrow/go/v14/arrow"
)

type Plugin struct {
	config       Config
	resultSchema []schema.StreamSchema
	ctx          *stream_context.Context
}

func NewOpenAIPlugin(appctx *stream_context.Context, config Config) (*Plugin, error) {
	return &Plugin{config: config, ctx: appctx}, nil
}

func (p *Plugin) Process(context context.Context, msg message.Message) (message.Message, error) {
	if msg.GetStream() != p.config.StreamName {
		return msg, nil
	}

	msg.SetNewField(p.config.TargetField, "ALLOW", arrow.BinaryTypes.String)

	return msg, nil
}

// EvolveSchema will add a string column to the schema in order to store OpenAI response
func (p *Plugin) EvolveSchema(streamSchema *schema.StreamSchemaObj) error {
	streamSchema.AddField(p.config.StreamName, p.config.TargetField, arrow.BinaryTypes.String, message.ArrowToPg10(arrow.BinaryTypes.String))
	return nil
}
