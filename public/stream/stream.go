package stream

import (
	"astro/config"
	"astro/internal/sources"
	"astro/internal/stream_context"
	"context"
	"errors"
	"github.com/reactivex/rxgo/v2"
)

type Stream struct {
	ctx              *stream_context.Context
	stream           chan rxgo.Item
	observableStream rxgo.Observable

	sinks  []SinkWrapper
	source *SourceWrapper
}

func InitFromConfig(config config.Configuration) (*Stream, error) {
	streamContext := stream_context.CreateContext()
	streamContext.Logger.Info("Bootstrapping Astro Stream-ETL")
	if config.Service.InfluxEnabled {
		metrics, err := loadInfluxMetrics(config.Service.Influx)
		if err != nil {
			streamContext.Logger.WithPrefix("metrics").Error("failed to load influx metrics")
			return nil, err
		}
		streamContext.SetMetrics(metrics)
	}

	s := &Stream{}
	s.ctx = streamContext
	s.stream = make(chan rxgo.Item)
	s.observableStream = rxgo.FromChannel(s.stream)

	streamContext.Logger.Info("Loading up source driver")
	sourceWrapper := NewSourceWrapper(config.Source.Driver, config)
	sinkWrapper := NewSinkWrapper(config.Sink.Driver, config)
	s.source = &sourceWrapper
	s.sinks = append(s.sinks, sinkWrapper)

	return s, nil
}

func InitManually() (*Stream, error) {
	s := Stream{}
	s.ctx = stream_context.CreateContext()
	s.stream = make(chan rxgo.Item)
	s.observableStream = rxgo.FromChannel(s.stream)
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
		err := sink.Connect()
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
	go func() {
		for {
			select {
			case producerMessage := <-s.source.Events():
				s.stream <- rxgo.Of(producerMessage.Message)
			}
		}
	}()

	s.observableStream.Connect(context.Background())
	for v := range s.observableStream.Observe() {
		for _, sink := range s.sinks {
			sink.Write(v.V.(sources.MessageEvent))
		}
	}
	select {}
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
