package websockets

import (
	"context"

	"github.com/charmbracelet/log"
	"github.com/gorilla/websocket"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sinks"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/message"
)

type SinkPlugin struct {
	wsClient     *websocket.Conn
	streamSchema []schema.StreamSchema
	config       Config
	logger       *log.Logger
}

func NewWebSocketSinkPlugin(config Config, schema []schema.StreamSchema, appCtx *stream_context.Context) sinks.DataSink {
	return &SinkPlugin{
		streamSchema: schema,
		config:       config,
		logger:       appCtx.Logger.WithPrefix("[sink]: websocket"),
	}
}

func (s *SinkPlugin) Connect(ctx context.Context) error {
	var client *websocket.Conn
	client, _, err := websocket.DefaultDialer.Dial(s.config.Url, nil)
	if err != nil {
		return err
	}

	go func(c *websocket.Conn) {
		for {
			if _, _, cerr := c.NextReader(); cerr != nil {
				c.Close()
				break
			}
		}
	}(client)

	s.wsClient = client
	return err
}

func (s *SinkPlugin) Write(message *message.Message) error {
	encodedMessage := message.AsJSONString()
	return s.wsClient.WriteMessage(websocket.BinaryMessage, []byte(encodedMessage))
}

func (s *SinkPlugin) GetType() sinks.SinkDriver {
	return sinks.WebSocketSinkType
}

func (s *SinkPlugin) SetExpectedSchema(schema []schema.StreamSchema) {

}

func (s *SinkPlugin) Stop() {}
