package log

import (
	"context"
	"github.com/charmbracelet/log"
	"github.com/usedatabrew/blink/internal/message"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/stream_context"
)

type Plugin struct {
	config Config
	ctx    *stream_context.Context
	log    *log.Logger
}

func NewLogPlugin(appctx *stream_context.Context, config Config) (*Plugin, error) {
	return &Plugin{config: config, ctx: appctx, log: log.WithPrefix("log-processor")}, nil
}

func (p *Plugin) Process(context context.Context, msg *message.Message) (*message.Message, error) {
	if p.config.StreamName == "*" || msg.GetStream() == p.config.StreamName {
		jsonMarshaledMessage, _ := msg.Data.MarshalJSON()
		p.log.Info("message received", "message", string(jsonMarshaledMessage))
	}

	return msg, nil
}

// EvolveSchema will not be executed for
func (p *Plugin) EvolveSchema(streamSchema *schema.StreamSchemaObj) error {

	return nil
}
