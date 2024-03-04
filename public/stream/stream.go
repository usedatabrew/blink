package stream

import (
	"errors"
	"fmt"
	"github.com/usedatabrew/blink/config"
	"github.com/usedatabrew/blink/internal/offset_storage"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/service_registry"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/message"
	"github.com/usedatabrew/tango"
	"sync"
	"time"
)

type Stream struct {
	ctx  *stream_context.Context
	lock sync.Mutex

	schema   *schema.StreamSchemaObj
	registry *service_registry.Registry

	processors []ProcessorWrapper
	sinks      []SinkWrapper
	source     *SourceWrapper
}

func InitFromConfig(config config.Configuration) (*Stream, error) {
	streamContext := stream_context.CreateContext(config.Service.PipelineId)
	streamContext.Logger.Info("Bootstrapping blink Stream-ETL")

	if config.Service.OffsetStorageURI != "" {
		offsetStorage := offset_storage.NewOffsetStorage(config.Service.OffsetStorageURI)
		fmt.Println(offsetStorage)
		streamContext.SetOffsetStorage(offsetStorage)
	}

	var processorList []string
	for _, proc := range config.Processors {
		processorList = append(processorList, string(proc.Driver))
	}
	if config.Service.InfluxEnabled {
		metrics, err := loadInfluxMetrics(config.Service.Influx)
		if err != nil {
			streamContext.Logger.WithPrefix("Metrics").Error("failed to load influx metrics")
			return nil, err
		}
		metrics.RegisterProcessors(processorList)
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
		metrics.RegisterProcessors(processorList)
		streamContext.SetMetrics(metrics)
		streamContext.Logger.WithPrefix("Metrics").Info("Component has been loaded")
	}

	s := &Stream{}
	s.ctx = streamContext
	if config.Service.ETCD != nil {
		s.registry = service_registry.NewServiceRegistry(s.ctx, *config.Service.ETCD, int(config.Service.PipelineId))
		s.registry.Start()
	}

	sourceWrapper := NewSourceWrapper(config.Source.Driver, config)
	s.source = &sourceWrapper

	streamContext.Logger.WithPrefix("Source").With(
		"driver", config.Source.Driver,
	).Info("Loaded")

	s.schema = schema.NewStreamSchemaObj(config.Source.StreamSchema)
	for _, processorCfg := range config.Processors {
		procWrapper := NewProcessorWrapper(processorCfg.Driver, processorCfg.Config, s.ctx)
		s.processors = append(s.processors, procWrapper)
		streamContext.Logger.WithPrefix("Processors").
			With("driver", processorCfg.Driver).
			Info("Loaded")
	}

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

	if config.Service.ETCD != nil {
		s.registry.SetState(service_registry.Loaded)
	}

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
	var messageSent = 0
	var messagesReceived = 0

	go func() {
		for {
			time.Sleep(time.Second * 10)
			s.ctx.Logger.WithPrefix("Stream").Info(
				"Messages stat", "messages_received", messagesReceived,
				"messages_sent", messageSent, "messages_dropped_or_filtered", messagesReceived-messageSent,
			)
		}
	}()

	dataStream := tango.NewTango()

	var dataStreamStages []tango.Stage
	for idx, _ := range s.processors {
		// this is required as idx is a ref,
		// so we will end up with the case when only the last processor will be executed
		// as many times as we have processors
		procIndex := idx
		stage := tango.Stage{
			Channel: make(chan interface{}),
			Function: func(i interface{}) (interface{}, error) {
				switch i.(type) {
				case *message.Message:
					if i.(*message.Message) == nil {
						return nil, nil
					}
					return s.processors[procIndex].Process(i.(*message.Message))
				}
				return nil, nil
			},
		}
		dataStreamStages = append(dataStreamStages, stage)
	}

	sinkStage := tango.Stage{
		Channel: make(chan interface{}),
		Function: func(i interface{}) (interface{}, error) {
			switch i.(type) {
			case *message.Message:
				inMessage := i.(*message.Message)
				if inMessage == nil {
					return nil, nil
				}

				err := s.sinks[0].Write(inMessage)
				if err != nil {
					s.ctx.Logger.WithPrefix("sink").Errorf("failed to write to sink %v", err)
				} else {
					messageSent += 1
				}
				return nil, err
			}

			return nil, nil
		},
	}
	dataStreamStages = append(dataStreamStages, sinkStage)
	dataStream.SetStages(dataStreamStages)

	streamProxyChan := make(chan interface{})
	dataStream.SetProducerChannel(streamProxyChan)
	dataStream.OnProcessed(func(i interface{}, err error) {

	})

	go func() {
		for {
			select {
			case sourceEvent := <-s.source.Events():
				if sourceEvent.Err != nil {
					s.ctx.Logger.Errorf("Error processing message %s", sourceEvent.Err.Error())
				} else {
					streamProxyChan <- sourceEvent.Message
					messagesReceived += 1
				}
			}
		}
	}()

	go s.source.Start()
	if s.registry != nil {
		s.registry.SetState(service_registry.Started)
	}

	return dataStream.Start()
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
