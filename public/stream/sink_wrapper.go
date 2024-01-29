package stream

import (
	"github.com/usedatabrew/blink/config"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sinks"
	"github.com/usedatabrew/blink/internal/sinks/kafka"
	"github.com/usedatabrew/blink/internal/sinks/mongodb"
	"github.com/usedatabrew/blink/internal/sinks/nats"
	"github.com/usedatabrew/blink/internal/sinks/postgres"
	"github.com/usedatabrew/blink/internal/sinks/rabbit_mq"
	"github.com/usedatabrew/blink/internal/sinks/stdout"
	websocket "github.com/usedatabrew/blink/internal/sinks/websockets"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/message"
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

func (p *SinkWrapper) Write(msg *message.Message) error {
	err := p.sinkDriver.Write(msg)
	if err != nil {
		p.ctx.Metrics.IncrementSinkErrCounter()
	} else {
		p.ctx.Metrics.IncrementSentCounter()
	}

	return err
}

func (p *SinkWrapper) SetExpectedSchema(s *schema.StreamSchemaObj) {
	p.sinkDriver.SetExpectedSchema(s.GetLatestSchema())
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
		return stdout.NewStdOutSinkPlugin(driverConfig, cfg.Source.StreamSchema, p.ctx)
	case sinks.KafkaSinkType:
		driverConfig, err := ReadDriverConfig[kafka.Config](cfg.Sink.Config, kafka.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return kafka.NewKafkaSinkPlugin(driverConfig, cfg.Source.StreamSchema)
	case sinks.WebSocketSinkType:
		driverConfig, err := ReadDriverConfig[websocket.Config](cfg.Sink.Config, websocket.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return websocket.NewWebSocketSinkPlugin(driverConfig, cfg.Source.StreamSchema, p.ctx)
	case sinks.MongoDBSinkType:
		driverConfig, err := ReadDriverConfig[mongodb.Config](cfg.Sink.Config, mongodb.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return mongodb.NewMongoDBSinkPlugin(driverConfig, cfg.Source.StreamSchema, p.ctx)
	case sinks.PostgresSinkType:
		driverConfig, err := ReadDriverConfig[postgres.Config](cfg.Sink.Config, postgres.Config{})
		if err != nil {
			panic("can read driver config")
		}
		return postgres.NewPostgresSinkPlugin(driverConfig, cfg.Source.StreamSchema, p.ctx)
	case sinks.NatsSinkType:
		driverConfig, err := ReadDriverConfig[nats.Config](cfg.Sink.Config, nats.Config{})

		if err != nil {
			panic("can't read driver config")
		}

		return nats.NewNatsSinkPlugin(driverConfig, cfg.Source.StreamSchema, p.ctx)
	case sinks.RabbitMqSinkType:
		driverConfig, err := ReadDriverConfig[rabbit_mq.Config](cfg.Sink.Config, rabbit_mq.Config{})

		if err != nil {
			panic("can't read driver config")
		}

		return rabbit_mq.NewRabbitMqSinkPlugin(driverConfig, cfg.Source.StreamSchema, p.ctx)
	default:
		p.ctx.Logger.WithPrefix("Sink loader").Fatal("Failed to load driver", "driver", driver)
	}

	return nil
}
