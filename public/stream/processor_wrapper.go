package stream

import (
	"errors"
	"time"

	"github.com/usedatabrew/blink/config"
	"github.com/usedatabrew/blink/internal/metrics"
	"github.com/usedatabrew/blink/internal/processors"
	"github.com/usedatabrew/blink/internal/processors/ai_content_moderation"
	"github.com/usedatabrew/blink/internal/processors/http"
	logProc "github.com/usedatabrew/blink/internal/processors/log"
	"github.com/usedatabrew/blink/internal/processors/openai"
	sqlproc "github.com/usedatabrew/blink/internal/processors/sql"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/message"
)

// ProcessorWrapper wraps processor writer plugin in order to
// measure performance, build proper configuration and control the context
type ProcessorWrapper struct {
	processorDriver processors.DataProcessor
	ctx             *stream_context.Context
	metrics         metrics.Metrics
	procDriver      string
}

func NewProcessorWrapper(pluginType processors.ProcessorDriver, config interface{}, appctx *stream_context.Context) ProcessorWrapper {
	loader := ProcessorWrapper{
		metrics:    appctx.Metrics,
		procDriver: string(pluginType),
	}
	loader.ctx = appctx
	loadedDriver, err := loader.LoadDriver(pluginType, config)
	if err != nil {
		appctx.Logger.Fatalf("Failed to load driver %v", err)
	}
	loader.processorDriver = loadedDriver
	return loader
}

func (p *ProcessorWrapper) Process(msg *message.Message) (*message.Message, error) {
	p.metrics.IncrementProcessorReceivedMessages(p.procDriver)
	execStart := time.Now()
	procMsg, err := p.processorDriver.Process(p.ctx.GetContext(), msg)
	if err == nil {
		p.metrics.IncrementProcessorSentMessages(p.procDriver)
	}
	if err == nil && procMsg == nil {
		p.metrics.IncrementProcessorDroppedMessages(p.procDriver)
	}

	execEnd := time.Since(execStart)
	p.metrics.SetProcessorExecutionTime(p.procDriver, execEnd.Milliseconds())
	return procMsg, err
}

func (p *ProcessorWrapper) EvolveSchema(s *schema.StreamSchemaObj) error {
	return p.processorDriver.EvolveSchema(s)
}

func (p *ProcessorWrapper) SetStreamContext(ctx *stream_context.Context) {
	p.ctx = ctx
}

func (p *ProcessorWrapper) LoadDriver(driver processors.ProcessorDriver, cfg interface{}) (processors.DataProcessor, error) {
	switch driver {
	case processors.OpenAIProcessor:
		driverConfig, err := config.ReadDriverConfig[openai.Config](cfg, openai.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return openai.NewOpenAIPlugin(p.ctx, driverConfig)
	case processors.SQLProcessor:
		driverConfig, err := config.ReadDriverConfig[sqlproc.Config](cfg, sqlproc.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return sqlproc.NewSqlTransformPlugin(p.ctx, driverConfig)
	case processors.HttpProcessor:
		driverConfig, err := config.ReadDriverConfig[http.Config](cfg, http.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return http.NewHttpPlugin(p.ctx, driverConfig)
	case processors.AIContentModeratorProcessor:
		driverConfig, err := config.ReadDriverConfig[ai_content_moderation.Config](cfg, ai_content_moderation.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return ai_content_moderation.NewAIContentModerationPlugin(p.ctx, driverConfig)
	case processors.LogProcessor:
		driverConfig, err := config.ReadDriverConfig[logProc.Config](cfg, logProc.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return logProc.NewLogPlugin(p.ctx, driverConfig)
	default:
		return nil, errors.New("unregistered driver provided")
	}

}
