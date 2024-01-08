package sources

import (
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/usedatabrew/blink/internal/helper"
	"github.com/usedatabrew/blink/internal/schema"
)

func BuildOutputSchema(inputSchema []schema.StreamSchema) map[string]*arrow.Schema {
	outputSchemas := make(map[string]*arrow.Schema)
	for _, collection := range inputSchema {
		var outputSchemaFields []arrow.Field
		for _, col := range collection.Columns {
			outputSchemaFields = append(outputSchemaFields, arrow.Field{
				Name:     col.Name,
				Type:     helper.MapPlainTypeToArrow(col.DatabrewType),
				Nullable: col.Nullable,
				Metadata: arrow.Metadata{},
			})
		}
		outputSchema := arrow.NewSchema(outputSchemaFields, nil)
		outputSchemas[collection.StreamName] = outputSchema
	}

	return outputSchemas
}
