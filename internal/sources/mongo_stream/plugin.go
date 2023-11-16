package mongo_stream

import (
	"astro/internal/message"
	"astro/internal/schema"
	"astro/internal/sources"
	"context"
	"fmt"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/cloudquery/plugin-sdk/v4/scalar" // todo: check it
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
	instance := &SourcePlugin{
		config:        config,
		inputSchema:   schema,
		messageStream: make(chan sources.MessageEvent),
	}

	instance.buildOutputSchema()

	return instance
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
		fmt.Println("start taking snapshot for", v.StreamName)

		cursor, err := p.database.Collection(v.StreamName).Find(p.ctx, bson.D{})

		if err != nil {
			panic(err)
		}

		defer cursor.Close(p.ctx)

		for cursor.Next(p.ctx) {
			var data bson.M

			if err := cursor.Decode(&data); err != nil {
				panic(err)
			}

			p.process(v.StreamName, data)
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

			p.process(v.StreamName, data)
		}
	}
}

func (p *SourcePlugin) process(stream string, data map[string]interface{}) {
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

	for i, v := range p.outputSchema[stream].Fields() {
		value := eventData[v.Name]

		s := scalar.NewScalar(p.outputSchema[stream].Field(i).Type)

		if err := s.Set(value); err != nil {
			panic(err)
		}

		scalar.AppendToBuilder(builder.Field(i), s)
	}

	m := message.New(builder.NewRecord())
	m.SetEvent(eventOperation)
	m.SetStream(stream)

	p.messageStream <- sources.MessageEvent{
		Message: m,
		Err:     nil,
	}
}

func (p *SourcePlugin) buildOutputSchema() {
	outputSchemas := make(map[string]*arrow.Schema)
	for _, collection := range p.inputSchema {
		var outputSchemaFields []arrow.Field
		for _, col := range collection.Columns {
			outputSchemaFields = append(outputSchemaFields, arrow.Field{
				Name: col.Name,
				//Type:     helpers.MapPlainTypeToArrow(col.DatabrewType),
				Type:     arrow.BinaryTypes.String,
				Nullable: col.Nullable,
				Metadata: arrow.Metadata{},
			})
		}
		outputSchema := arrow.NewSchema(outputSchemaFields, nil)
		outputSchemas[collection.StreamName] = outputSchema
	}

	p.outputSchema = outputSchemas
}
