package stream

import (
	"astro/config"
	"astro/internal/message"
	"astro/internal/sinks"
	"astro/internal/sinks/kafka"
	"astro/internal/sinks/stdout"
	websocket "astro/internal/sinks/websockets"
	"astro/internal/stream_context"
)

// SinkWrapper wraps plan sink writer plugin in order to
// measure performance, build proper configuration and control the context
type SinkWrapper struct {
	sinkDriver sinks.DataSink
	ctx        *stream_context.Context
}

func NewSinkWrapper(pluginType sinks.SinkDriver, config config.Configuration, appctx *stream_context.Context) SinkWrapper {
	loader := SinkWrapper{}
	loader.ctx = appctx
	loadedDriver := loader.LoadDriver(pluginType, config)
	loader.sinkDriver = loadedDriver
	return loader
}

func (p *SinkWrapper) Init() error {
	return p.sinkDriver.Connect(p.ctx.GetContext())
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
	switch driver {
	case sinks.StdOutSinkType:
		driverConfig, err := ReadDriverConfig[stdout.Config](cfg.Sink.Config, stdout.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return stdout.NewStdOutSinkPlugin(driverConfig, cfg.Service.StreamSchema, p.ctx)
	case sinks.KafkaSinkType:
		driverConfig, err := ReadDriverConfig[kafka.Config](cfg.Sink.Config, kafka.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return kafka.NewKafkaSinkPlugin(driverConfig, cfg.Service.StreamSchema)
	case sinks.WebSocketSinkType:
		driverConfig, err := ReadDriverConfig[websocket.Config](cfg.Sink.Config, websocket.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return websocket.NewWebSocketSinkPlugin(driverConfig, cfg.Service.StreamSchema, p.ctx)
	}

	return nil
}
