package websockets

import (
	"context"
	"github.com/charmbracelet/log"
	"github.com/gorilla/websocket"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sources"
)

type SourcePlugin struct {
	wsClient      *websocket.Conn
	streamSchema  []schema.StreamSchema
	config        Config
	logger        *log.Logger
	messageStream chan sources.MessageEvent
}

func NewWebSocketSourcePlugin(config Config, schema []schema.StreamSchema) sources.DataSource {
	return &SourcePlugin{
		streamSchema:  schema,
		config:        config,
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
	go func(c *websocket.Conn) {
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				panic(err)
			}
			log.Printf("received: %s", message)
		}
	}(s.wsClient)
}

func (s *SourcePlugin) Events() chan sources.MessageEvent {
	return s.messageStream
}

func (s *SourcePlugin) Stop() {
	s.wsClient.Close()
}
