package openai

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/charmbracelet/log"
	"github.com/sashabaranov/go-openai"
	"github.com/usedatabrew/blink/internal/helper"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/message"
	"sync"
	"time"
)

type Plugin struct {
	config                        Config
	ctx                           *stream_context.Context
	client                        *openai.Client
	model                         string
	prompt                        string
	rateLimiterTick               *time.Ticker
	logger                        *log.Logger
	mutx                          sync.Mutex
	messagesProcessedWithinALimit int64
}

func NewOpenAIPlugin(appctx *stream_context.Context, config Config) (*Plugin, error) {
	plugin := &Plugin{
		config: config, ctx: appctx,
		client: openai.NewClient(config.ApiKey),
		model:  config.Model, prompt: config.Prompt,
		logger: log.WithPrefix("processor [openai]: "),
	}

	if config.LimitPerMinute > 0 {
		plugin.rateLimiterTick = time.NewTicker(time.Minute)
	}

	return plugin, nil
}

func (p *Plugin) Process(context context.Context, msg *message.Message) (*message.Message, error) {
	if msg.GetStream() != p.config.StreamName {
		return msg, nil
	}
	//if p.rateLimiterTick != nil && p.messagesProcessedWithinALimit >= p.config.LimitPerMinute {
	//	p.logger.Info("Rate limit wait")
	//	<-p.rateLimiterTick.C
	//	p.mutx.Lock()
	//	p.messagesProcessedWithinALimit = 0
	//	p.mutx.Unlock()
	//}

	processedMessage, err := p.processMessage(context, msg)
	if err != nil {
		return nil, err
	}

	p.mutx.Lock()
	p.messagesProcessedWithinALimit += 1
	p.mutx.Unlock()
	return processedMessage, nil
}

func (p *Plugin) processMessage(context context.Context, msg *message.Message) (*message.Message, error) {
	sourceFieldValue := msg.Data.AccessProperty(p.config.SourceField)

	command := "You are data pipeline assistant. You take the data from the user and perform various checks and " +
		"analysis. You are capable of checking the data for different patterns, harmful content, etc " +
		"You must strictly follow a given instruction:" + p.prompt +
		"Your responses must always be short, without any explanation, unless your wants you to do so." +
		"You should never explain your thoughts or process. You need to respond with an answer only" +
		"Yours response will most likely used in database field to try to assume the correct form of response based on the instructions given"

	resp, err := p.client.CreateChatCompletion(
		context,
		openai.ChatCompletionRequest{
			Model: p.model,
			Messages: []openai.ChatCompletionMessage{
				{
					Role:    openai.ChatMessageRoleUser,
					Content: fmt.Sprintf("%v", sourceFieldValue),
				},
				{
					Role:    openai.ChatMessageRoleAssistant,
					Content: command,
				},
			},
		},
	)

	if err != nil {
		return nil, err
	}

	msg.Data.SetProperty(p.config.TargetField, resp.Choices[0].Message.Content)

	return msg, nil
}

// EvolveSchema will add a string column to the schema in order to store OpenAI response
func (p *Plugin) EvolveSchema(streamSchema *schema.StreamSchemaObj) error {
	streamSchema.AddField(p.config.StreamName, p.config.TargetField, arrow.BinaryTypes.String, helper.ArrowToPg10(arrow.BinaryTypes.String))
	return nil
}
