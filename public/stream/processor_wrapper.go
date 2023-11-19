package stream

import (
	"astro/internal/message"
	"astro/internal/processors"
	"astro/internal/processors/openai"
	"astro/internal/sources"
	"astro/internal/stream_context"
	"errors"
)

// ProcessorWrapper wraps plan sink writer plugin in order to
// measure performance, build proper configuration and control the context
type ProcessorWrapper struct {
	processorDriver processors.DataProcessor
	ctx             *stream_context.Context
}

func NewProcessorWrapper(pluginType processors.ProcessorDriver, config interface{}, appctx *stream_context.Context) ProcessorWrapper {
	loader := ProcessorWrapper{}
	loader.ctx = appctx
	loadedDriver, err := loader.LoadDriver(pluginType, config)
	if err != nil {
		appctx.Logger.Fatalf("Failed to load driver %v", err)
	}
	loader.processorDriver = loadedDriver
	return loader
}

func (p *ProcessorWrapper) Process(msg sources.MessageEvent) (message.Message, error) {
	return p.processorDriver.Process(p.ctx.GetContext(), msg.Message)
}

func (p *ProcessorWrapper) SetStreamContext(ctx *stream_context.Context) {
	p.ctx = ctx
}

func (p *ProcessorWrapper) LoadDriver(driver processors.ProcessorDriver, cfg interface{}) (processors.DataProcessor, error) {
	switch driver {
	case processors.OpenAIProcessor:
		driverConfig, err := ReadDriverConfig[openai.Config](cfg, openai.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return openai.NewOpenAIPlugin(p.ctx, driverConfig)
	default:
		return nil, errors.New("unregistered driver provided")
	}

}
