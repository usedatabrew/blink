package stream

import (
	"github.com/usedatabrew/blink/config"
	"github.com/usedatabrew/blink/internal/sources"
	"github.com/usedatabrew/blink/internal/sources/airtable"
	"github.com/usedatabrew/blink/internal/sources/kafka"
	"github.com/usedatabrew/blink/internal/sources/mongo_stream"
	"github.com/usedatabrew/blink/internal/sources/mysql_cdc"
	"github.com/usedatabrew/blink/internal/sources/playground"
	"github.com/usedatabrew/blink/internal/sources/postgres_cdc"
	"github.com/usedatabrew/blink/internal/sources/postgres_incr_sync"
	"github.com/usedatabrew/blink/internal/sources/websockets"
	"github.com/usedatabrew/blink/internal/stream_context"
)

// SourceWrapper wraps source plugin in order to
// measure performance, build proper configuration and control the context
type SourceWrapper struct {
	sourceDriver sources.DataSource
	pluginType   sources.SourceDriver
	config       config.Configuration
	stream       chan sources.MessageEvent
	ctx          *stream_context.Context
}

func NewSourceWrapper(pluginType sources.SourceDriver, config config.Configuration) SourceWrapper {
	loader := SourceWrapper{
		stream:     make(chan sources.MessageEvent),
		config:     config,
		pluginType: pluginType,
	}

	return loader
}

func (p *SourceWrapper) Init(appctx *stream_context.Context) error {
	p.ctx = appctx
	loadedDriver := p.LoadDriver(p.pluginType, p.config)
	p.sourceDriver = loadedDriver
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

func (p *SourceWrapper) LoadDriver(driver sources.SourceDriver, fcg config.Configuration) sources.DataSource {
	switch driver {
	case sources.Playground:
		driverConfig, err := config.ReadDriverConfig[playground.Config](fcg.Source.Config, playground.Config{})
		if err != nil {
			panic("cannot read driver config")
		}
		return playground.NewPlaygroundSourcePlugin(driverConfig, fcg.Source.StreamSchema)
	case sources.PostgresCDC:
		driverConfig, err := config.ReadDriverConfig[postgres_cdc.Config](fcg.Source.Config, postgres_cdc.Config{})
		if err != nil {
			panic("cannot read driver config")
		}
		return postgres_cdc.NewPostgresSourcePlugin(driverConfig, fcg.Source.StreamSchema)
	case sources.WebSockets:
		driverConfig, err := config.ReadDriverConfig[websockets.Config](fcg.Source.Config, websockets.Config{})
		if err != nil {
			panic("cannot read driver config")
		}
		return websockets.NewWebSocketSourcePlugin(driverConfig, fcg.Source.StreamSchema)
	case sources.MongoStream:
		driverConfig, err := config.ReadDriverConfig[mongo_stream.Config](fcg.Source.Config, mongo_stream.Config{})

		if err != nil {
			panic("cannot read driver config")
		}

		return mongo_stream.NewMongoStreamSourcePlugin(driverConfig, fcg.Source.StreamSchema)
	case sources.AirTable:
		driverConfig, err := config.ReadDriverConfig[airtable.Config](fcg.Source.Config, airtable.Config{})

		if err != nil {
			panic("cannot read driver config")
		}

		return airtable.NewAirTableSourcePlugin(driverConfig, fcg.Source.StreamSchema)
	case sources.MysqlCDC:
		driverConfig, err := config.ReadDriverConfig[mysql_cdc.Config](fcg.Source.Config, mysql_cdc.Config{})

		if err != nil {
			panic("cannot read driver config")
		}

		return mysql_cdc.NewMysqlSourcePlugin(driverConfig, fcg.Source.StreamSchema)
	case sources.PostgresIncremental:
		driverConfig, err := config.ReadDriverConfig[postgres_incr_sync.Config](fcg.Source.Config, postgres_incr_sync.Config{})

		if err != nil {
			panic("cannot read driver config")
		}

		return postgres_incr_sync.NewPostgresIncrSourcePlugin(p.ctx, driverConfig, fcg.Source.StreamSchema)
	case sources.Kafka:
		driverConfig, err := config.ReadDriverConfig[kafka.Config](fcg.Source.Config, kafka.Config{})

		if err != nil {
			panic("cannot read driver config")
		}

		return kafka.NewKafkaSourcePlugin(driverConfig, fcg.Source.StreamSchema)
	default:
		p.ctx.Logger.WithPrefix("Source driver loader").Fatal("Failed to load driver", "driver", driver)
	}

	return nil
}
