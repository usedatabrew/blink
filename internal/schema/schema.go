package schema

import (
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/usedatabrew/blink/internal/helper"
	"sort"
)

// StreamSchema represents YAML configuration of the
// source plugin schema. It's used to build Apache Arrow schema from this definition
type StreamSchema struct {
	StreamName string   `yaml:"stream"`
	Columns    []Column `yaml:"columns"`
}

type Column struct {
	Name                string   `yaml:"name"`
	DatabrewType        string   `yaml:"databrewType"`
	NativeConnectorType string   `yaml:"nativeConnectorType"`
	PK                  bool     `yaml:"pk"`
	Nullable            bool     `yaml:"nullable"`
	Columns             []Column `yaml:"columns,omitempty"`
}

func (s *StreamSchema) SortColumnsAsc() {
	sort.Sort(ByName(s.Columns))
}

func (s *StreamSchema) AsArrow() *arrow.Schema {
	var arrowSchema *arrow.Schema
	var outputSchemaFields []arrow.Field
	for _, column := range s.Columns {
		var field arrow.Field
		if helper.IsPrimitiveType(column.DatabrewType) {
			field = arrow.Field{
				Name:     column.Name,
				Type:     helper.MapPlainTypeToArrow(column.DatabrewType),
				Nullable: column.Nullable,
				Metadata: arrow.Metadata{},
			}
		} else {
			// trying to handle complex field type.
			// like array of numbers of nested object
			// List<JSON>, JSON, List<Int64>, List<Float54>, List<String>
			if column.DatabrewType == "List<JSON>" || column.DatabrewType == "JSON" {
				field = buildFieldsFromJson(column)
			} else {
				field = buildFieldsFromList(column)
			}
		}

		outputSchemaFields = append(outputSchemaFields, field)
	}

	arrowSchema = arrow.NewSchema(outputSchemaFields, nil)
	return arrowSchema
}

func remove[T any](slice []T, s int) []T {
	return append(slice[:s], slice[s+1:]...)
}

func buildFieldsFromList(column Column) arrow.Field {
	field := arrow.Field{
		Name:     column.Name,
		Nullable: column.Nullable,
		Metadata: arrow.Metadata{},
	}

	switch column.DatabrewType {
	case "List<Int64>":
		field.Type = arrow.ListViewOf(arrow.PrimitiveTypes.Int64)
	case "List<Float64>":
		field.Type = arrow.ListViewOf(arrow.PrimitiveTypes.Float64)
	case "List<String>":
		field.Type = arrow.ListViewOf(arrow.BinaryTypes.String)
	default:
		field.Type = arrow.ListViewOf(arrow.BinaryTypes.String)
	}

	return field
}

func buildFieldsFromJson(column Column) arrow.Field {
	field := arrow.Field{
		Name:     column.Name,
		Nullable: column.Nullable,
		Metadata: arrow.Metadata{},
	}

	var nestedFields []arrow.Field
	for _, subCol := range column.Columns {
		subColField := arrow.Field{
			Name:     subCol.Name,
			Nullable: subCol.Nullable,
			Metadata: arrow.Metadata{},
		}

		if subCol.DatabrewType == "JSON" {
			subColField = buildFieldsFromJson(subCol)
		} else if subCol.DatabrewType == "List<Int64>" || subCol.DatabrewType == "List<Float64>" || subCol.DatabrewType == "List<String>" {
			subColField = buildFieldsFromList(subCol)
		} else {
			subColField.Type = helper.MapPlainTypeToArrow(subCol.DatabrewType)
		}

		nestedFields = append(nestedFields, subColField)
	}

	if column.DatabrewType == "JSON" {
		field.Type = arrow.StructOf(nestedFields...)
	} else if column.DatabrewType == "List<JSON>" {
		// else means json only column
		field.Type = arrow.ListViewOf(arrow.StructOf(nestedFields...))
	}

	return field
}
