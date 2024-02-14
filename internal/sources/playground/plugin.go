package playground

import (
	"context"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/charmbracelet/log"
	"github.com/goccy/go-json"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sources"
	"github.com/usedatabrew/message"
	"time"
)

type SourcePlugin struct {
	messageEvents chan sources.MessageEvent
	config        Config
	schema        []schema.StreamSchema
	logger        *log.Logger
	ctx           context.Context
}

func NewPlaygroundSourcePlugin(config Config, s []schema.StreamSchema) sources.DataSource {
	plugin := &SourcePlugin{
		config:        config,
		schema:        s,
		messageEvents: make(chan sources.MessageEvent),
		logger:        log.WithPrefix("[source]: Playground"),
	}

	return plugin
}

func (s *SourcePlugin) Connect(ctx context.Context) error {
	if s.config.DataType != "user" && s.config.DataType != "market" {
		s.logger.Fatal("Unsupported data type provided. Need 'market' or 'user' data type in the config")
	}

	s.ctx = ctx
	return nil
}

func (s *SourcePlugin) Start() {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, s.schema[0].AsArrow())
	if s.config.HistoricalBatch {
		for i := 0; i <= 1000; i++ {
			msgBytes := s.getFakeMessageForStream()

			if err := json.Unmarshal(msgBytes, &builder); err != nil {
				s.messageEvents <- sources.MessageEvent{
					Message: nil,
					Err:     err,
				}
				continue
			}

			data, _ := builder.NewRecord().MarshalJSON()
			m := message.NewMessage(message.Snapshot, s.config.DataType, data)
			s.messageEvents <- sources.MessageEvent{
				Message: m,
				Err:     nil,
			}
		}
	}

	ticker := time.NewTicker(time.Second * time.Duration(s.config.PublishIntervalSeconds))
	var err error

	for {
		select {
		case <-ticker.C:
			msgBytes := s.getFakeMessageForStream()

			if err = json.Unmarshal(msgBytes, &builder); err != nil {
				s.messageEvents <- sources.MessageEvent{
					Message: nil,
					Err:     err,
				}
				continue
			}

			data, _ := builder.NewRecord().MarshalJSON()
			m := message.NewMessage(message.Insert, s.config.DataType, data)
			s.messageEvents <- sources.MessageEvent{
				Message: m,
				Err:     nil,
			}
		}
	}
}

func (s *SourcePlugin) Events() chan sources.MessageEvent {
	return s.messageEvents
}

func (s *SourcePlugin) Stop() {}

func (s *SourcePlugin) getFakeMessageForStream() []byte {
	if s.config.DataType == "user" {
		return generateUserDataJsonBytes()
	}

	return generateMarketDataJsonBytes()
}
