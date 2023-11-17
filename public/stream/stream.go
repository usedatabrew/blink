package stream

import (
	"astro/config"
	"astro/internal/sources"
	"astro/internal/stream_context"
	"errors"
	"fmt"
	"github.com/reactivex/rxgo/v2"
	"sync"
	"time"
)

type Stream struct {
	ctx              *stream_context.Context
	stream           chan rxgo.Item
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
	s.stream = make(chan rxgo.Item)
	s.observableStream = rxgo.FromChannel(s.stream)

	streamContext.Logger.WithPrefix("Source").With(
		"driver", config.Source.Driver,
	).Info("Loading driver")
	sourceWrapper := NewSourceWrapper(config.Source.Driver, config)
	s.source = &sourceWrapper

	streamContext.Logger.WithPrefix("Source").With(
		"driver", config.Source.Driver,
	).Info("Loaded")

	streamContext.Logger.WithPrefix("Sinks").With(
		"driver", config.Sink.Driver,
	).Info("Loading driver")
	sinkWrapper := NewSinkWrapper(config.Sink.Driver, config, s.ctx)
	streamContext.Logger.WithPrefix("Sinks").With(
		"driver", config.Sink.Driver,
	).Info("Loaded")

	if err := s.SetSinks([]SinkWrapper{sinkWrapper}); err != nil {
		s.ctx.Logger.WithPrefix("Sinks").Errorf("failed to initialize sinks for pipeline %v", err)
		return nil, err
	}

	return s, nil
}

func InitManually() (*Stream, error) {
	s := Stream{}
	s.ctx = stream_context.CreateContext()
	s.stream = make(chan rxgo.Item)
	//s.observableStream = rxgo.FromChannel(s.stream)
	s.observableStream = rxgo.FromChannel(s.stream, rxgo.WithBufferedChannel(5))
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

	var messagesProcessed = 0

	go func() {
		for {
			time.Sleep(time.Second * 5)
			fmt.Println("Messages processed", messagesProcessed)
		}
	}()

	go s.source.Start()
	go func() {
		for {
			select {
			case producerMessage := <-s.source.Events():
				s.stream <- rxgo.Of(producerMessage)
			}
		}
	}()

	s.observableStream.Connect(s.ctx.GetContext())
	for event := range s.observableStream.Observe() {
		err := s.sinks[0].Write(event.V.(sources.MessageEvent).Message)
		if err != nil {
			s.ctx.Logger.WithPrefix("sink").Errorf("failed to write to sink %v", err)
		} else {
			messagesProcessed += 1
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
