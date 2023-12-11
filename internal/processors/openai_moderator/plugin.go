package openai

import (
	"context"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/usedatabrew/blink/internal/message"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/stream_context"
)

type Plugin struct {
	config       Config
	resultSchema []schema.StreamSchema
	ctx          *stream_context.Context
	apiKey       string
}

func NewOpenAIModeratorPlugin(appctx *stream_context.Context, config Config) (*Plugin, error) {
	return &Plugin{config: config, ctx: appctx, apiKey: config.ApiKey}, nil
}

func (p *Plugin) Process(context context.Context, msg message.Message) (message.Message, error) {
	if msg.GetStream() != p.config.StreamName {
		return msg, nil
	}

	sourceFieldValue := msg.GetValue(p.config.SourceField)

	msg.SetNewField(p.config.TargetField, resp.Choices[0].Message.Content, arrow.BinaryTypes.String)

	return msg, nil
}

// EvolveSchema will add a string column to the schema in order to store OpenAI response
func (p *Plugin) EvolveSchema(streamSchema *schema.StreamSchemaObj) error {
	streamSchema.AddField(p.config.StreamName, p.config.TargetField, arrow.BinaryTypes.String, message.ArrowToPg10(arrow.BinaryTypes.String))
	return nil
}
