package stream

import (
	"astro/config"
	"astro/internal/schema"
	"astro/internal/service_registry"
	"astro/internal/sources"
	"astro/internal/stream_context"
	"errors"
	"sync"
	"time"

	"github.com/mariomac/gostream/stream"
)

type Stream struct {
	ctx  *stream_context.Context
	lock sync.Mutex

	schema   *schema.StreamSchemaObj
	registry *service_registry.Registry

	processors []ProcessorWrapper
	sinks      []SinkWrapper
	source     *SourceWrapper

	dataStream stream.Stream[sources.MessageEvent]
}

func InitFromConfig(config config.Configuration) (*Stream, error) {
	streamContext := stream_context.CreateContext()
	streamContext.Logger.Info("Bootstrapping Astro Stream-ETL")
	if config.Service.InfluxEnabled {
		metrics, err := loadInfluxMetrics(config.Service.Influx)
		if err != nil {
			streamContext.Logger.WithPrefix("Metrics").Error("failed to load influx metrics")
			return nil, err
		}
		streamContext.SetMetrics(metrics)
		streamContext.Logger.WithPrefix("Metrics").Info("Component has been loaded")
	} else {
		streamContext.Logger.WithPrefix("Metrics").Warn("No influx config has been provided. Fallback to local prometheus metrics")
		// fallback to local prometheus metrics
		metrics, err := loadPrometheusMetrics(config)
		if err != nil {
			streamContext.Logger.WithPrefix("Metrics").Error("failed to load local prometheus metrics")
			return nil, err
		}
		streamContext.SetMetrics(metrics)
		streamContext.Logger.WithPrefix("Metrics").Info("Component has been loaded")
	}

	s := &Stream{}
	s.ctx = streamContext
	s.registry = service_registry.NewServiceRegistry(s.ctx, config.Service.ETCD, config.Service.PipelineId)
	s.registry.Start()

	streamContext.Logger.WithPrefix("Source").With(
		"driver", config.Source.Driver,
	).Info("Loading...")

	sourceWrapper := NewSourceWrapper(config.Source.Driver, config)
	s.source = &sourceWrapper

	streamContext.Logger.WithPrefix("Source").With(
		"driver", config.Source.Driver,
	).Info("Loaded")

	s.schema = schema.NewStreamSchemaObj(config.Service.StreamSchema)
	for _, processorCfg := range config.Processors {
		streamContext.Logger.WithPrefix("Processors").
			With("driver", processorCfg.Driver).
			Info("Loading...")
		procWrapper := NewProcessorWrapper(processorCfg.Driver, processorCfg.Config, s.ctx)
		s.processors = append(s.processors, procWrapper)
		streamContext.Logger.WithPrefix("Processors").
			With("driver", processorCfg.Driver).
			Info("Loaded")
	}

	streamContext.Logger.WithPrefix("Sinks").With(
		"driver", config.Sink.Driver,
	).Info("Loading...")
	sinkWrapper := NewSinkWrapper(config.Sink.Driver, config, s.ctx)
	streamContext.Logger.WithPrefix("Sinks").With(
		"driver", config.Sink.Driver,
	).Info("Loaded")

	if err := s.SetSinks([]SinkWrapper{sinkWrapper}); err != nil {
		s.ctx.Logger.WithPrefix("Sinks").Errorf("failed to initialize sinks for pipeline %v", err)
		return nil, err
	}

	s.evolveSchemaForSinks(s.schema)
	s.sinks[0].SetExpectedSchema(s.schema)

	s.dataStream = stream.OfChannel(s.source.Events())
	s.registry.SetState(service_registry.Loaded)

	return s, nil
}

func (s *Stream) SetProducer(producer SourceWrapper) error {
	if err := producer.Init(s.ctx); err != nil {
		return err
	}

	s.source = &producer

	return nil
}

func (s *Stream) SetSinks(sinks []SinkWrapper) error {
	for _, sink := range sinks {
		err := sink.Init()
		if err != nil {
			return err
		}
		s.sinks = append(s.sinks, sink)
	}

	return nil
}

func (s *Stream) Start() error {
	if err := s.validateAndInit(); err != nil {
		return err
	}
	go s.source.Start()
	var messagesProcessed = 0

	go func() {
		for {
			time.Sleep(time.Second * 25)
			s.ctx.Logger.WithPrefix("Stream").Infof("Total messages processed: %d", messagesProcessed)
		}
	}()

	for _, proc := range s.processors {
		s.dataStream = s.dataStream.Map(func(event sources.MessageEvent) sources.MessageEvent {
			result, err := proc.Process(event)
			if err != nil {

				panic(err)
			}

			return sources.MessageEvent{
				Message: result,
				Err:     err,
			}
		})
	}

	s.registry.SetState(service_registry.Started)
	s.dataStream.ForEach(func(event sources.MessageEvent) {
		err := s.sinks[0].Write(event.Message)
		if err != nil {
			s.ctx.Logger.WithPrefix("sink").Errorf("failed to write to sink %v", err)
		} else {
			messagesProcessed += 1
		}
	})

	return nil
}

func (s *Stream) validateAndInit() error {
	if s.source == nil {
		s.ctx.Logger.Error("Source is required to start pipeline")
		return errors.New("source is not set")
	}

	if len(s.sinks) == 0 {
		s.ctx.Logger.Error("At least one sink should be defined to start pipeline")
		return errors.New("at least one sink should be set")
	}

	err := s.source.Init(s.ctx)
	if err != nil {
		return err
	}

	return nil
}

func (s *Stream) evolveSchemaForSinks(streamSchema *schema.StreamSchemaObj) {
	for _, processor := range s.processors {
		err := processor.EvolveSchema(streamSchema)
		if err != nil {
			s.ctx.Logger.Fatalf("error evolving schema %s", err.Error())
		}

	}
}
