package message

import (
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/charmbracelet/log"
	"reflect"
)

func inferArrowType(value interface{}) arrow.DataType {
	switch value.(type) {
	case int, int8, int16, int32, int64:
		return arrow.PrimitiveTypes.Int64
	case uint, uint8, uint16, uint32, uint64:
		return arrow.PrimitiveTypes.Uint64
	case float32, float64:
		return arrow.PrimitiveTypes.Float64
	case string:
		return arrow.BinaryTypes.String
	default:
		// You may need to handle additional types based on your use case
		log.Fatalf("Unsupported data type: %T", value)
		return nil
	}
}

// getValue extracts the value at the specified row index from a column using reflection
func getValue(column arrow.Array, rowIndex int) interface{} {
	switch column.(type) {
	case *array.Int8:
		return column.(*array.Int8).Value(rowIndex)
	case *array.Int16:
		return column.(*array.Int16).Value(rowIndex)
	case *array.Int32:
		return column.(*array.Int32).Value(rowIndex)
	case *array.Int64:
		return column.(*array.Int64).Value(rowIndex)
	case *array.Uint8:
		return column.(*array.Uint8).Value(rowIndex)
	case *array.Uint16:
		return column.(*array.Uint16).Value(rowIndex)
	case *array.Uint32:
		return column.(*array.Uint32).Value(rowIndex)
	case *array.Uint64:
		return column.(*array.Uint64).Value(rowIndex)
	case *array.Float16:
		return column.(*array.Float16).Value(rowIndex)
	case *array.Float32:
		return column.(*array.Float32).Value(rowIndex)
	case *array.Float64:
		return column.(*array.Float64).Value(rowIndex)
	case *array.String:
		return column.(*array.String).Value(rowIndex)
	case *array.Binary:
		return column.(*array.Binary).Value(rowIndex)
	case *array.Boolean:
		return column.(*array.Boolean).Value(rowIndex)
	case *array.Date32:
		return column.(*array.Date32).Value(rowIndex)
	case *array.Date64:
		return column.(*array.Date64).Value(rowIndex)
	case *array.Timestamp:
		return column.(*array.Timestamp).Value(rowIndex)
	default:
		log.Fatalf("Unsupported column type: %v", reflect.TypeOf(column))
		return nil
	}
}
