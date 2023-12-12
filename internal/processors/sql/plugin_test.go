package sqlproc

import (
	"fmt"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/stream_context"
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

	plugin, err := NewSqlTransformlugin(stream_context.CreateContext(), Config{
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

	plugin, err = NewSqlTransformlugin(stream_context.CreateContext(), Config{
		Query: "SELECT destination from stream.flightas",
	})

	err = plugin.EvolveSchema(pluginSchema)
	if err.Error() != "select from undefined stream" {
		t.Fatal()
	}
}
