package mongo_stream

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/goccy/go-json"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sources"
	"github.com/usedatabrew/message"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type SourcePlugin struct {
	ctx           context.Context
	config        Config
	client        *mongo.Client
	database      *mongo.Database
	inputSchema   []schema.StreamSchema
	outputSchema  map[string]*arrow.Schema
	messageStream chan sources.MessageEvent
}

func NewMongoStreamSourcePlugin(config Config, schema []schema.StreamSchema) sources.DataSource {
	return &SourcePlugin{
		config:        config,
		inputSchema:   schema,
		outputSchema:  sources.BuildOutputSchema(schema),
		messageStream: make(chan sources.MessageEvent),
	}
}

func (p *SourcePlugin) Connect(ctx context.Context) error {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(p.config.Uri))

	if err != nil {
		return err
	}

	p.client = client
	p.ctx = ctx

	p.database = client.Database(p.config.Database)

	return nil
}

func (p *SourcePlugin) Start() {
	if p.config.StreamSnapshot {
		go p.takeSnapshot()
	} else {
		go p.watch()
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
	for _, v := range p.inputSchema {
		filter := bson.D{}
		opts := options.Find().SetSort(bson.M{"_id": "1"})
		cursor, err := p.database.Collection(v.StreamName).Find(p.ctx, filter, opts)

		if err != nil {
			panic(err)
		}

		defer cursor.Close(p.ctx)

		for cursor.Next(p.ctx) {
			var data bson.M

			if err := cursor.Decode(&data); err != nil {
				panic(err)
			}

			p.process(v.StreamName, data, true)
		}
	}

	p.watch()
}

func (p *SourcePlugin) watch() {
	for _, v := range p.inputSchema {
		fmt.Println("start watching changes for", v.StreamName)

		collection := p.database.Collection(v.StreamName)

		stream, err := collection.Watch(p.ctx, mongo.Pipeline{}, options.ChangeStream().SetFullDocument(options.UpdateLookup))

		if err != nil {
			panic(err)
		}

		defer stream.Close(p.ctx)

		for stream.Next(p.ctx) {
			var data bson.M

			if err := stream.Decode(&data); err != nil {
				panic(err)
			}

			p.process(v.StreamName, data, false)
		}
	}
}

func (p *SourcePlugin) process(stream string, data map[string]interface{}, snapshot bool) {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, p.outputSchema[stream])

	var eventOperation string
	var eventData map[string]interface{}

	if operation, ok := data["operationType"]; ok {
		switch operation {
		case "delete":
			eventOperation = operation.(string)
			eventData = data["documentKey"].(bson.M)
		default:
			eventOperation = operation.(string)
			eventData = data["fullDocument"].(bson.M)
		}
	} else {
		eventOperation = "insert"
		eventData = data
	}

	if snapshot {
		eventOperation = string(message.Snapshot)
	}

	encodedJson, _ := json.Marshal(&eventData)
	err := json.Unmarshal(encodedJson, &builder)
	// TODO:: rewrite
	if err != nil {
		panic(err)
	}

	mbytes, _ := builder.NewRecord().MarshalJSON()
	m := message.NewMessage(message.Event(eventOperation), stream, mbytes)

	p.messageStream <- sources.MessageEvent{
		Message: m,
		Err:     nil,
	}
}
