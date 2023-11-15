package stream

import (
	"astro/config"
	"astro/internal/message"
	"astro/internal/sinks"
	"astro/internal/sinks/stdout"
	"astro/internal/stream_context"
	"fmt"
)

// SinkWrapper wraps plan sink writer plugin in order to
// measure performance, build proper configuration and control the context
type SinkWrapper struct {
	sinkDriver sinks.DataSink
	ctx        *stream_context.Context
}

func NewSinkWrapper(pluginType sinks.SinkDriver, config config.Configuration) SinkWrapper {
	loader := SinkWrapper{}
	loadedDriver := loader.LoadDriver(pluginType, config)
	loader.sinkDriver = loadedDriver
	return loader
}

func (p *SinkWrapper) Init(appctx *stream_context.Context) error {
	p.ctx = appctx
	return p.sinkDriver.Connect(appctx.GetContext())
}

func (p *SinkWrapper) Write(msg message.Message) error {
	err := p.sinkDriver.Write(msg)
	if err != nil {
		p.ctx.Metrics.IncrementSinkErrCounter()
	} else {
		p.ctx.Metrics.IncrementSentCounter()
	}

	return err
}

func (p *SinkWrapper) SetStreamContext(ctx *stream_context.Context) {
	p.ctx = ctx
}

func (p *SinkWrapper) LoadDriver(driver sinks.SinkDriver, cfg config.Configuration) sinks.DataSink {
	fmt.Println("loading sink", driver)
	switch driver {
	case sinks.StdOutSinkType:
		driverConfig, err := ReadDriverConfig[stdout.Config](cfg.Sink.Config, stdout.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return stdout.NewStdOutSinkPlugin(driverConfig, cfg.Service.StreamSchema)
	}

	return nil
}
