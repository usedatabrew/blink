package stream

import (
	"astro/config"
	"astro/internal/sources"
	"astro/internal/sources/mongo_stream"
	"astro/internal/sources/postgres_cdc"
	"astro/internal/stream_context"
	"context"
	"fmt"

	"gopkg.in/yaml.v3"
)

// SourceWrapper wraps plan producer plugin in order to
// measure performance, build proper configuration and control the context
type SourceWrapper struct {
	sourceDriver sources.DataSource
	ctx          *stream_context.Context
}

func NewSourceWrapper(pluginType sources.SourceDriver, config config.Configuration) SourceWrapper {
	loader := SourceWrapper{}
	fmt.Println(config.Service.StreamSchema)
	loadedDriver := loader.LoadDriver(pluginType, config)
	loader.sourceDriver = loadedDriver
	return loader
}

func (p *SourceWrapper) Init() error {
	return p.sourceDriver.Connect(context.TODO())
}

func (p *SourceWrapper) Events() (stream chan sources.MessageEvent) {
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
}

func (p *SourceWrapper) SetStreamContext(ctx *stream_context.Context) {
	p.ctx = ctx
}

func (p *SourceWrapper) GetPluginConfigs(driver sources.SourceDriver, config *yaml.Node) (any, error) {
	switch driver {
	case sources.PostgresCDC:
		return ReadDriverConfig[postgres_cdc.Config](config, postgres_cdc.Config{})
	case sources.MongoStream:
		return ReadDriverConfig[mongo_stream.Config](config, mongo_stream.Config{})
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
	case sources.MongoStream:
		driverConfig, err := ReadDriverConfig[mongo_stream.Config](config.Source.Config, mongo_stream.Config{})

		if err != nil {
			panic("cannot ready driver config")
		}

		return mongo_stream.NewMongoStreamSourcePlugin(driverConfig, config.Service.StreamSchema)
	}

	return nil
}
