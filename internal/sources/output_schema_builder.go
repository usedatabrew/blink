package sources

import (
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/usedatabrew/blink/internal/schema"
)

func BuildOutputSchema(inputSchema []schema.StreamSchema) map[string]*arrow.Schema {
	outputSchemas := make(map[string]*arrow.Schema)
	for _, collection := range inputSchema {
		outputSchema := collection.AsArrow()
		outputSchemas[collection.StreamName] = outputSchema
	}

	return outputSchemas
}
