package stream

import (
	"astro/config"
	"astro/internal/sources"
	"astro/internal/stream_context"
	"errors"

	"github.com/reactivex/rxgo/v2"
	"sync"
)

type Stream struct {
	ctx              *stream_context.Context
	stream           chan sources.MessageEvent
	observableStream rxgo.Observable
	lock             sync.Mutex

	sinks  []SinkWrapper
	source *SourceWrapper
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
	}

	s := &Stream{}
	s.ctx = streamContext
	s.stream = make(chan sources.MessageEvent)

	streamContext.Logger.WithPrefix("Source").Info("Loading driver")
	sourceWrapper := NewSourceWrapper(config.Source.Driver, config)
	s.source = &sourceWrapper

	streamContext.Logger.WithPrefix("Source").Info("Loaded")

	streamContext.Logger.WithPrefix("Sinks").Info("Loading driver")
	sinkWrapper := NewSinkWrapper(config.Sink.Driver, config, s.ctx)
	streamContext.Logger.WithPrefix("Sinks").Info("Loaded")

	if err := s.SetSinks([]SinkWrapper{sinkWrapper}); err != nil {
		s.ctx.Logger.WithPrefix("Sinks").Errorf("failed to initialize sinks for pipeline %v", err)
		return nil, err
	}

	return s, nil
}

func InitManually() (*Stream, error) {
	s := Stream{}
	s.ctx = stream_context.CreateContext()
	s.stream = make(chan sources.MessageEvent)
	return &s, nil
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
	for {
		select {
		case producerMessage := <-s.source.Events():
			err := s.sinks[0].Write(producerMessage.Message)
			if err != nil {
				s.ctx.Logger.WithPrefix("sink").Errorf("failed to write to sink %v", err)
			}
		}
	}
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
