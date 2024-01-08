package websockets

import (
	"context"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/charmbracelet/log"
	"github.com/cloudquery/plugin-sdk/v4/scalar"
	"github.com/goccy/go-json"
	"github.com/gorilla/websocket"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sources"
	"github.com/usedatabrew/message"
)

type SourcePlugin struct {
	wsClient     *websocket.Conn
	streamSchema []schema.StreamSchema
	outputSchema map[string]*arrow.Schema
	config       Config
	logger       *log.Logger
	// webSocket plugin doesn't support multiple streams
	stream        string
	messageStream chan sources.MessageEvent
}

func NewWebSocketSourcePlugin(config Config, schema []schema.StreamSchema) sources.DataSource {
	return &SourcePlugin{
		streamSchema:  schema,
		config:        config,
		outputSchema:  sources.BuildOutputSchema(schema),
		stream:        schema[0].StreamName,
		messageStream: make(chan sources.MessageEvent),
		logger:        log.WithPrefix("[source]: websocket"),
	}
}

func (s *SourcePlugin) Connect(ctx context.Context) error {
	var client *websocket.Conn
	client, _, err := websocket.DefaultDialer.Dial(s.config.Url, nil)
	if err != nil {
		return err
	}

	s.wsClient = client
	return err
}

func (s *SourcePlugin) Start() {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, s.outputSchema[s.stream])

	go func(c *websocket.Conn) {
		for {
			_, wsMessage, err := c.ReadMessage()
			if err != nil {
				s.logger.Fatal(err)
			}
			var rawMessage map[string]interface{}
			err = json.Unmarshal(wsMessage, &rawMessage)
			if err != nil {
				s.logger.Fatal(err)
			}
			for i, v := range s.outputSchema[s.stream].Fields() {
				value := rawMessage[v.Name]
				ss := scalar.NewScalar(s.outputSchema[s.stream].Field(i).Type)
				if err := ss.Set(value); err != nil {
					panic(err)
				}

				scalar.AppendToBuilder(builder.Field(i), ss)
			}

			mBytes, _ := builder.NewRecord().MarshalJSON()
			m := message.NewMessage(message.Insert, s.stream, mBytes)

			s.messageStream <- sources.MessageEvent{
				Message: m,
				Err:     nil,
			}
		}
	}(s.wsClient)
}

func (s *SourcePlugin) Events() chan sources.MessageEvent {
	return s.messageStream
}

func (s *SourcePlugin) Stop() {
	s.wsClient.Close()
}
