package mongodb

import (
	"context"
	"github.com/charmbracelet/log"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sinks"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/message"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type SinkPlugin struct {
	appCtx                  *stream_context.Context
	streamSchema            []schema.StreamSchema
	config                  Config
	logger                  *log.Logger
	client                  *mongo.Client
	database                *mongo.Database
	topLevelPkNameByStreams map[string]string
}

func NewMongoDBSinkPlugin(config Config, schema []schema.StreamSchema, appCtx *stream_context.Context) sinks.DataSink {
	return &SinkPlugin{
		streamSchema:            schema,
		config:                  config,
		appCtx:                  appCtx,
		topLevelPkNameByStreams: make(map[string]string),
		logger:                  appCtx.Logger.WithPrefix("[sink]: mongodb"),
	}
}

func (s *SinkPlugin) Connect(ctx context.Context) error {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(s.config.Uri))

	if err != nil {
		return err
	}

	s.client = client

	s.database = client.Database(s.config.Database)
	return err
}

func (s *SinkPlugin) Write(message *message.Message) error {
	pkForStream := s.topLevelPkNameByStreams[message.GetStream()]
	if pkForStream != "" {
		filter := bson.M{pkForStream: message.Data.AccessProperty(pkForStream)}
		upsert := true
		upsertModel := &mongo.UpdateOneModel{
			Filter: filter,
			Upsert: &upsert,
			Update: bson.M{"$set": message.Data.JsonQ().First()},
		}
		_, err := s.database.Collection(message.GetStream()).
			BulkWrite(s.appCtx.GetContext(), []mongo.WriteModel{upsertModel})
		return err
	}

	_, err := s.database.Collection(message.GetStream()).InsertOne(s.appCtx.GetContext(), message.Data.JsonQ().First())
	return err
}

func (s *SinkPlugin) GetType() sinks.SinkDriver {
	return sinks.MongoDBSinkType
}

// SetExpectedSchema for Stdout component does nothing, since this component is used mostly for debugging
func (s *SinkPlugin) SetExpectedSchema(schema []schema.StreamSchema) {
	for _, stream := range schema {
		name := stream.StreamName
		var pkName string
		for _, col := range stream.Columns {
			if col.PK {
				pkName = col.Name
				break
			}
		}
		s.topLevelPkNameByStreams[name] = pkName
	}
}

func (s *SinkPlugin) Stop() {
	s.appCtx.GetContext().Done()
}
