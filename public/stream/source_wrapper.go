package stream

import (
	"github.com/usedatabrew/blink/config"
	"github.com/usedatabrew/blink/internal/sources"
	"github.com/usedatabrew/blink/internal/sources/airtable"
	"github.com/usedatabrew/blink/internal/sources/mongo_stream"
	"github.com/usedatabrew/blink/internal/sources/playground"
	"github.com/usedatabrew/blink/internal/sources/postgres_cdc"
	"github.com/usedatabrew/blink/internal/sources/websockets"
	"github.com/usedatabrew/blink/internal/stream_context"

	"gopkg.in/yaml.v3"
)

// SourceWrapper wraps plan source plugin in order to
// measure performance, build proper configuration and control the context
type SourceWrapper struct {
	sourceDriver sources.DataSource
	stream       chan sources.MessageEvent
	ctx          *stream_context.Context
}

func NewSourceWrapper(pluginType sources.SourceDriver, config config.Configuration) SourceWrapper {
	loader := SourceWrapper{
		stream: make(chan sources.MessageEvent),
	}
	loadedDriver := loader.LoadDriver(pluginType, config)
	loader.sourceDriver = loadedDriver
	return loader
}

func (p *SourceWrapper) Init(appctx *stream_context.Context) error {
	p.ctx = appctx
	return p.sourceDriver.Connect(appctx.GetContext())
}

func (p *SourceWrapper) Events() chan sources.MessageEvent {
	return p.stream
}

func (p *SourceWrapper) Start() {
	go func() {
		for {
			select {
			case event := <-p.sourceDriver.Events():
				if event.Err != nil {
					p.ctx.Metrics.IncrementSourceErrCounter()
				} else {
					p.ctx.Metrics.IncrementReceivedCounter()
				}
				p.stream <- event
			}
		}
	}()

	p.sourceDriver.Start()
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
	case sources.Playground:
		driverConfig, err := ReadDriverConfig[playground.Config](config.Source.Config, playground.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return playground.NewPlaygroundSourcePlugin(driverConfig, config.Source.StreamSchema)
	case sources.PostgresCDC:
		driverConfig, err := ReadDriverConfig[postgres_cdc.Config](config.Source.Config, postgres_cdc.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return postgres_cdc.NewPostgresSourcePlugin(driverConfig, config.Source.StreamSchema)
	case sources.WebSockets:
		driverConfig, err := ReadDriverConfig[websockets.Config](config.Source.Config, websockets.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return websockets.NewWebSocketSourcePlugin(driverConfig, config.Source.StreamSchema)
	case sources.MongoStream:
		driverConfig, err := ReadDriverConfig[mongo_stream.Config](config.Source.Config, mongo_stream.Config{})

		if err != nil {
			panic("cannot ready driver config")
		}

		return mongo_stream.NewMongoStreamSourcePlugin(driverConfig, config.Source.StreamSchema)
	case sources.AirTable:
		driverConfig, err := ReadDriverConfig[airtable.Config](config.Source.Config, airtable.Config{})

		if err != nil {
			panic("cannot ready driver config")
		}

		return airtable.NewAirTableSourcePlugin(driverConfig, config.Source.StreamSchema)
	default:
		p.ctx.Logger.WithPrefix("Source driver loader").Fatal("Failed to load driver", "driver", driver)
	}

	return nil
}
