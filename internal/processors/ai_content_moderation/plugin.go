package ai_content_moderation

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/sashabaranov/go-openai"
	"github.com/usedatabrew/blink/internal/helper"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/message"
)

type Plugin struct {
	config Config
	ctx    *stream_context.Context
	client *openai.Client
}

func NewAIContentModerationPlugin(appctx *stream_context.Context, config Config) (*Plugin, error) {
	return &Plugin{config: config, ctx: appctx, client: openai.NewClient(config.ApiKey)}, nil
}

func (p *Plugin) Process(context context.Context, msg *message.Message) (*message.Message, error) {
	if msg.GetStream() != p.config.StreamName {
		return msg, nil
	}
	sourceFieldValue := msg.Data.AccessProperty(p.config.SourceField)

	moderationResponse, err := p.client.Moderations(p.ctx.GetContext(), openai.ModerationRequest{
		Input: sourceFieldValue.(string),
		Model: openai.ModerationTextStable,
	})

	if err != nil {
		fmt.Printf("OpenAI moderation error: %v\n", err)
		if err != nil {
			return msg, nil
		}
	}

	result := fmt.Sprintf("%v", moderationResponse.Results[0].Flagged)
	msg.Data.SetProperty(p.config.TargetField, result)

	return msg, nil
}

// EvolveSchema will add a string column to the schema in order to store OpenAI response
func (p *Plugin) EvolveSchema(streamSchema *schema.StreamSchemaObj) error {
	streamSchema.AddField(p.config.StreamName, p.config.TargetField, arrow.BinaryTypes.String, helper.ArrowToPg10(arrow.BinaryTypes.String))
	return nil
}
