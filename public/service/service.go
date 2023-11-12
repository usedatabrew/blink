package service

import (
	"context"
	"github.com/reactivex/rxgo/v2"
	"lunaflow/internal/sinks"
	"lunaflow/internal/sources"
)

type Service struct {
	ctx              context.Context
	stream           chan rxgo.Item
	observableStream rxgo.Observable

	sinks    []sinks.DataSink
	producer sources.DataSource
}

func InitService() (*Service, error) {
	s := Service{}
	s.ctx = context.Background()
	s.stream = make(chan rxgo.Item)
	s.observableStream = rxgo.FromChannel(s.stream)
	return &s, nil
}

func (s *Service) SetProducer(producer sources.DataSource) error {
	if err := producer.Connect(s.ctx); err != nil {
		return err
	}

	s.producer = producer

	return nil
}

func (s *Service) SetSinks(sinks []sinks.DataSink) error {
	for _, sink := range sinks {
		err := sink.Connect(context.Background())
		if err != nil {
			return err
		}
		s.sinks = append(s.sinks, sink)
	}

	return nil
}

func (s *Service) Start() error {
	go s.producer.Start()
	go func() {
		for {
			select {
			case producerMessage := <-s.producer.Events():
				s.stream <- rxgo.Of(producerMessage)
			}
		}
	}()

	s.observableStream.Connect(context.Background())
	for v := range s.observableStream.Observe() {
		for _, sink := range s.sinks {
			sink.Write(v.V.(sources.MessageEvent).Message)
		}
	}
	select {}
}
