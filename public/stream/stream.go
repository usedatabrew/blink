package stream

import (
	"context"
	"github.com/reactivex/rxgo/v2"
	"lunaflow/internal/sinks"
	"lunaflow/internal/sources"
)

type Stream struct {
	ctx              context.Context
	stream           chan rxgo.Item
	observableStream rxgo.Observable

	sinks    []sinks.DataSink
	producer sources.DataSource
}

func InitService() (*Stream, error) {
	s := Stream{}
	s.ctx = context.Background()
	s.stream = make(chan rxgo.Item)
	s.observableStream = rxgo.FromChannel(s.stream)
	return &s, nil
}

func (s *Stream) SetProducer(producer sources.DataSource) error {
	if err := producer.Connect(s.ctx); err != nil {
		return err
	}

	s.producer = producer

	return nil
}

func (s *Stream) SetSinks(sinks []sinks.DataSink) error {
	for _, sink := range sinks {
		err := sink.Connect(context.Background())
		if err != nil {
			return err
		}
		s.sinks = append(s.sinks, sink)
	}

	return nil
}

func (s *Stream) Start() error {
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
