package stream

import (
	"astro/config"
	"astro/internal/sources"
	"astro/internal/sources/postgres_cdc"
	"astro/internal/stream_context"
	"gopkg.in/yaml.v3"
)

// SourceWrapper wraps plan source plugin in order to
// measure performance, build proper configuration and control the context
type SourceWrapper struct {
	sourceDriver sources.DataSource
	ctx          *stream_context.Context
}

func NewSourceWrapper(pluginType sources.SourceDriver, config config.Configuration) SourceWrapper {
	loader := SourceWrapper{}
	loadedDriver := loader.LoadDriver(pluginType, config)
	loader.sourceDriver = loadedDriver
	return loader
}

func (p *SourceWrapper) Init(appctx *stream_context.Context) error {
	p.ctx = appctx
	return p.sourceDriver.Connect(appctx.GetContext())
}

func (p *SourceWrapper) Events() chan sources.MessageEvent {
	stream := make(chan sources.MessageEvent)
	go func() {
		for {
			select {
			case event := <-p.sourceDriver.Events():
				if event.Err != nil {
					p.ctx.Metrics.IncrementSourceErrCounter()
				} else {
					p.ctx.Metrics.IncrementReceivedCounter()
				}
				stream <- event
			}
		}
	}()

	return stream
}

func (p *SourceWrapper) Start() {
	p.sourceDriver.Start()
}

func (p *SourceWrapper) SetStreamContext(ctx *stream_context.Context) {
	p.ctx = ctx
}

func (p *SourceWrapper) GetPluginConfigs(driver sources.SourceDriver, config *yaml.Node) (any, error) {
	switch driver {
	case sources.PostgresCDC:
		return ReadDriverConfig[postgres_cdc.Config](config, postgres_cdc.Config{})
	}

	return nil, nil
}

func (p *SourceWrapper) LoadDriver(driver sources.SourceDriver, config config.Configuration) sources.DataSource {
	switch driver {
	case sources.PostgresCDC:
		driverConfig, err := ReadDriverConfig[postgres_cdc.Config](config.Source.Config, postgres_cdc.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return postgres_cdc.NewPostgresSourcePlugin(driverConfig, config.Service.StreamSchema)
	}

	return nil
}
