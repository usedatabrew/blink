package mongodb

import (
	"context"
	"sync"
	"time"

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
	mutex                   sync.Mutex
	appCtx                  *stream_context.Context
	streamSchema            []schema.StreamSchema
	config                  Config
	logger                  *log.Logger
	client                  *mongo.Client
	database                *mongo.Database
	topLevelPkNameByStreams map[string]string
	prevSnapshotStream      string
	prevMessageEvent        message.Event
	messageBatchBuffer      []*message.Message
	messageBatchSize        int
	messageBatchIntervalMs  int64
	messageSnapshotTicker   *time.Timer
}

func NewMongoDBSinkPlugin(config Config, schema []schema.StreamSchema, appCtx *stream_context.Context) sinks.DataSink {
	return &SinkPlugin{
		streamSchema:            schema,
		config:                  config,
		appCtx:                  appCtx,
		messageBatchSize:        5000,
		messageBatchIntervalMs:  200,
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

func (s *SinkPlugin) Write(mess *message.Message) error {
	pkForStream := s.topLevelPkNameByStreams[mess.GetStream()]
	// Snapshot processing requires message processing in batches

	// Means we finished processing the snapshot. So it must be manually flushed before we can
	// move forward
	if s.prevMessageEvent == message.Snapshot && mess.GetEvent() != message.Snapshot {
		err := s.writeSnapshotBatch()
		if err != nil {
			return err
		}

		s.messageBatchBuffer = []*message.Message{}
		s.prevMessageEvent = mess.GetEvent()
	}

	if mess.GetEvent() == message.Snapshot {
		if s.prevSnapshotStream == "" {
			s.prevSnapshotStream = mess.GetStream()
		}
		// If our snapshots are different - we have to flush batch manually
		if mess.GetStream() != s.prevSnapshotStream {
			err := s.writeSnapshotBatch()
			if err != nil {
				return err
			}

			s.messageBatchBuffer = []*message.Message{}
			return nil
		}
		s.prevSnapshotStream = mess.GetStream()
		s.messageBatchBuffer = append(s.messageBatchBuffer, mess)
		if len(s.messageBatchBuffer) >= s.messageBatchSize {
			err := s.writeSnapshotBatch()
			if err != nil {
				return err
			}

			s.messageBatchBuffer = []*message.Message{}
			return nil
		} else if s.messageSnapshotTicker == nil {
			// Start a timer if not already running
			s.messageSnapshotTicker = time.AfterFunc(time.Millisecond*50, func() {
				s.mutex.Lock()
				defer s.mutex.Unlock()
				err := s.writeSnapshotBatch()
				if err != nil {
					panic("Failed to write snapshot batch")
				}
				s.messageBatchBuffer = []*message.Message{}
				s.messageSnapshotTicker.Stop()
				s.messageSnapshotTicker = nil
			})
			return nil
		} else {
			return nil
		}
	}

	// Triggers if source connector supports deletion mechanism
	if mess.GetEvent() == message.Delete && pkForStream != "" {
		filter := bson.M{pkForStream: mess.Data.AccessProperty(pkForStream)}
		deleteModel := &mongo.DeleteOneModel{
			Filter: filter,
		}
		_, err := s.database.Collection(s.config.StreamPrefix+mess.GetStream()).
			BulkWrite(s.appCtx.GetContext(), []mongo.WriteModel{deleteModel})
		return err
	}

	if pkForStream != "" {
		filter := bson.M{pkForStream: mess.Data.AccessProperty(pkForStream)}
		upsert := true
		upsertModel := &mongo.UpdateOneModel{
			Filter: filter,
			Upsert: &upsert,
			Update: bson.M{"$set": mess.Data.JsonQ().First()},
		}
		_, err := s.database.Collection(s.config.StreamPrefix+mess.GetStream()).
			BulkWrite(s.appCtx.GetContext(), []mongo.WriteModel{upsertModel})
		return err
	}

	_, err := s.database.Collection(s.config.StreamPrefix+mess.GetStream()).InsertOne(s.appCtx.GetContext(), mess.Data.JsonQ().First())
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
	s.client.Disconnect(s.appCtx.GetContext())
}

func (s *SinkPlugin) writeSnapshotBatch() error {
	if len(s.messageBatchBuffer) == 0 {
		return nil
	}

	firstMessage := s.messageBatchBuffer[0]
	var batchToWrite []interface{}
	for _, mess := range s.messageBatchBuffer {
		batchToWrite = append(batchToWrite, mess.Data.JsonQ().First())
	}
	_, err := s.database.Collection(s.config.StreamPrefix+firstMessage.GetStream()).InsertMany(s.appCtx.GetContext(), batchToWrite)
	return err
}
