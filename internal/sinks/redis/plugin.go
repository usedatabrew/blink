package redis

import (
	"context"
	"fmt"
	"github.com/charmbracelet/log"
	"github.com/redis/go-redis/v9"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sinks"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/message"
	"time"
)

type SinkPlugin struct {
	redisConn    *redis.Client
	appCtx       *stream_context.Context
	streamSchema []schema.StreamSchema
	config       Config
	logger       *log.Logger
	pksByStream  map[string]string
}

func NewRedisSinkPlugin(config Config, schema []schema.StreamSchema, appCtx *stream_context.Context) sinks.DataSink {
	return &SinkPlugin{
		streamSchema: schema,
		config:       config,
		appCtx:       appCtx,
		logger:       appCtx.Logger.WithPrefix("[sink]: redis"),
		pksByStream:  map[string]string{},
	}
}

func (s SinkPlugin) Connect(context context.Context) error {
	rdb := redis.NewClient(&redis.Options{
		Addr:     s.config.RedisAddr,
		Password: s.config.RedisPassword,
		DB:       0,
	})

	status := rdb.Ping(context)
	if status.Err() != nil {
		return status.Err()
	}

	s.redisConn = rdb
	return nil
}

func (s SinkPlugin) SetExpectedSchema(schema []schema.StreamSchema) {
	for _, stream := range schema {
		var pkCol string
		for _, c := range stream.Columns {
			if c.PK {
				pkCol = c.Name
			}
		}
		s.pksByStream[stream.StreamName] = pkCol
	}
}

func (s SinkPlugin) GetType() sinks.SinkDriver {
	return sinks.RedisSinkType
}

func (s SinkPlugin) Write(m *message.Message) error {
	streamName := m.GetStream()
	namespace := ""
	if s.config.CustomNamespace != "" {
		namespace = s.config.CustomNamespace
	}

	if s.config.NamespaceByStream {
		namespace = streamName
	}

	messageKey := fmt.Sprintf("%v", m.Data.AccessProperty(s.pksByStream[streamName]))
	if namespace != "" {
		messageKey = namespace + ":" + messageKey
	}

	payload := m.Data.JsonQ().First()
	ttl := time.Duration(0)
	if s.config.SetWithTTL > 0 {
		ttl = time.Second * time.Duration(s.config.SetWithTTL)
	}

	status := s.redisConn.Set(s.appCtx.GetContext(), messageKey, payload, ttl)

	return status.Err()
}

func (s SinkPlugin) Stop() {
	if err := s.redisConn.Close(); err != nil {
		s.logger.Fatal("Failed to close redis client", "error", err)
	}
}
