package openai

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/sashabaranov/go-openai"
	"github.com/usedatabrew/blink/internal/message"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/stream_context"
)

type Plugin struct {
	config Config
	ctx    *stream_context.Context
	client *openai.Client
	model  string
	prompt string
}

func NewOpenAIPlugin(appctx *stream_context.Context, config Config) (*Plugin, error) {
	return &Plugin{config: config, ctx: appctx, client: openai.NewClient(config.ApiKey), model: config.Model, prompt: config.Prompt}, nil
}

func (p *Plugin) Process(context context.Context, msg *message.Message) (*message.Message, error) {
	if msg.GetStream() != p.config.StreamName {
		return msg, nil
	}

	sourceFieldValue := msg.GetValue(p.config.SourceField)

	prompt := fmt.Sprintf("Take the data: %s and respond after doing following: %s . Provide the shortest response possible \n Do not explain your actions. If the question can be somehow answered with year/no - do exacetly that", sourceFieldValue, p.prompt)

	resp, err := p.client.CreateChatCompletion(
		p.ctx.GetContext(),
		openai.ChatCompletionRequest{
			Model: p.model,
			Messages: []openai.ChatCompletionMessage{
				{
					Role:    openai.ChatMessageRoleUser,
					Content: prompt,
				},
			},
		},
	)

	if err != nil {
		fmt.Printf("ChatCompletion error: %v\n", err)
		if err != nil {
			return msg, nil
		}
	}

	msg.SetNewField(p.config.TargetField, resp.Choices[0].Message.Content, arrow.BinaryTypes.String)

	return msg, nil
}

// EvolveSchema will add a string column to the schema in order to store OpenAI response
func (p *Plugin) EvolveSchema(streamSchema *schema.StreamSchemaObj) error {
	streamSchema.AddField(p.config.StreamName, p.config.TargetField, arrow.BinaryTypes.String, message.ArrowToPg10(arrow.BinaryTypes.String))
	return nil
}
