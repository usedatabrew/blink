package websockets

import (
	"astro/internal/message"
	"astro/internal/schema"
	"astro/internal/sinks"
	"astro/internal/stream_context"
	"context"
	"github.com/charmbracelet/log"
	"github.com/gorilla/websocket"
)

type SinkPlugin struct {
	wsClient     *websocket.Conn
	appCtx       *stream_context.Context
	streamSchema []schema.StreamSchema
	config       Config
	logger       *log.Logger
}

func NewWebSocketSinkPlugin(config Config, schema []schema.StreamSchema, appCtx *stream_context.Context) sinks.DataSink {
	return &SinkPlugin{
		streamSchema: schema,
		config:       config,
		appCtx:       appCtx,
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

func (s *SinkPlugin) Write(message message.Message) error {
	encodedMessage, _ := message.Data.MarshalJSON()
	return s.wsClient.WriteMessage(websocket.BinaryMessage, encodedMessage)
}

func (s *SinkPlugin) GetType() sinks.SinkDriver {
	return sinks.StdOutSinkType
}

func (s *SinkPlugin) SetExpectedSchema(schema []schema.StreamSchema) {

}

func (s *SinkPlugin) Stop() {
	s.appCtx.GetContext().Done()
}
