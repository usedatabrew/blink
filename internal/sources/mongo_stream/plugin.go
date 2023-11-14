package mongo_stream

import (
	"astro/internal/message"
	"astro/internal/schema"
	"astro/internal/sources"
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

type SourcePlugin struct {
	ctx           context.Context
	config        Config
	client        *mongo.Client
	database      *mongo.Database
	collection    *mongo.Collection
	streamSchema  []schema.StreamSchema
	messageStream chan sources.MessageEvent
}

func NewMongoStreamSourcePlugin(config Config, schema []schema.StreamSchema) sources.DataSource {
	return &SourcePlugin{
		config:        config,
		streamSchema:  schema,
		messageStream: make(chan sources.MessageEvent),
	}
}

func (p *SourcePlugin) Connect(ctx context.Context) error {
	client, err := mongo.Connect(p.ctx, options.Client().ApplyURI(p.config.Uri))

	if err != nil {
		return err
	}

	p.client = client
	p.ctx = ctx

	p.database = client.Database(p.config.Database)
	p.collection = p.database.Collection(p.config.Collection)

	return nil
}

func (p *SourcePlugin) Start() {
	if p.config.StreamSnapshot {
		go p.takeSnapshot()
	} else {
		p.watch()
	}
}

func (p *SourcePlugin) Stop() {
	if p.client != nil {
		p.client.Disconnect(p.ctx)
	}
}

func (p *SourcePlugin) Events() chan sources.MessageEvent {
	return p.messageStream
}

func (p *SourcePlugin) takeSnapshot() {
	cursor, err := p.collection.Find(p.ctx, bson.D{})

	if err != nil {
		panic(err)
	}

	defer cursor.Close(p.ctx)

	for cursor.Next(p.ctx) {
		var data bson.M

		if err := cursor.Decode(&data); err != nil {
			panic(err)
		}

		p.process(data)
	}

	p.watch()
}

func (p *SourcePlugin) watch() {
	stream, err := p.collection.Watch(p.ctx, mongo.Pipeline{}, options.ChangeStream().SetFullDocument(options.UpdateLookup))

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for stream.Next(p.ctx) {
		var data bson.M

		if err := stream.Decode(&data); err != nil {
			panic(err)
		}

		p.process(data)
	}

}

func (p *SourcePlugin) process(data map[string]interface{}) {
	event := make(map[string]interface{})

	event["Schema"] = p.database
	event["Table"] = p.collection

	if operation, ok := data["operationType"]; ok {
		switch operation {
		case "delete":
			event["Kind"] = operation
			event["Row"] = data["documentKey"]
		default:
			event["Kind"] = operation
			event["Row"] = data["fullDocument"]
		}
	} else {
		event["Kind"] = "insert"
		event["Row"] = data
	}

	tableSchema := arrow.NewSchema([]arrow.Field{
		{
			Name: "action",
			Type: arrow.PrimitiveTypes.String
		},
	})

	builder := array.NewRecordBuilder(memory.DefaultAllocator, tableSchema)

	p.messageStream <- sources.MessageEvent{
		Message: message.New(event),
		Err:     nil,
	}
}
