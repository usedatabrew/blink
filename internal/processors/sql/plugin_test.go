package sqlproc

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/cloudquery/plugin-sdk/v4/scalar"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/message"
	"testing"
)

func TestPlugin_EvolveSchema(t *testing.T) {
	pluginSchema := schema.NewStreamSchemaObj([]schema.StreamSchema{
		{
			StreamName: "flights",
			Columns: []schema.Column{
				{
					Name:         "flight_id",
					DatabrewType: "Int",
					Nullable:     false,
					PK:           true,
				},
				{
					Name:         "destination",
					DatabrewType: "String",
					Nullable:     false,
					PK:           false,
				},
			},
		},
	})

	plugin, err := NewSqlTransformPlugin(stream_context.CreateContext(), Config{
		Query: "SELECT destination from stream.flights",
	})
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	err = plugin.EvolveSchema(pluginSchema)
	if err != nil {
		t.Fatal(err)
	}

	if pluginSchema.GetLatestSchema()[0].Columns[0].Name != "destination" {
		t.Fatal("Schema should contain only one column")
	}

	plugin, err = NewSqlTransformPlugin(stream_context.CreateContext(), Config{
		Query: "SELECT destination from stream.flightas",
	})

	err = plugin.EvolveSchema(pluginSchema)
	if err.Error() != "select from undefined stream" {
		t.Fatal()
	}
}

func TestPlugin_Process(t *testing.T) {
	streamSchema := schema.NewStreamSchemaObj([]schema.StreamSchema{
		{
			StreamName: "test",
			Columns: []schema.Column{
				{
					Name:         "id",
					DatabrewType: "Int8",
				},
				{
					Name:         "user",
					DatabrewType: "String",
				},
			},
		},
	})
	updatedSchema := arrow.NewSchema([]arrow.Field{
		{
			Name: "id",
			Type: arrow.PrimitiveTypes.Int8,
		},
		{
			Name: "user",
			Type: arrow.BinaryTypes.String,
		},
	}, nil)
	updatedBuilder := array.NewRecordBuilder(memory.DefaultAllocator, updatedSchema)
	idScalar := scalar.NewScalar(arrow.PrimitiveTypes.Int8)
	if err := idScalar.Set(1); err != nil {
		panic(err)
	}
	scalar.AppendToBuilder(updatedBuilder.Field(0), idScalar)

	userScalar := scalar.NewScalar(arrow.BinaryTypes.String)
	if err := userScalar.Set("Maxym"); err != nil {
		panic(err)
	}
	scalar.AppendToBuilder(updatedBuilder.Field(1), userScalar)

	mbytes, _ := updatedBuilder.NewRecord().MarshalJSON()
	mess := message.NewMessage(message.Insert, "test", mbytes)

	plugin, err := NewSqlTransformPlugin(stream_context.CreateContext(), Config{
		Query: "SELECT id, user from stream.test WHERE id = 123",
	})
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	err = plugin.EvolveSchema(streamSchema)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	processedMessage, err := plugin.Process(context.Background(), mess)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(processedMessage)
}
