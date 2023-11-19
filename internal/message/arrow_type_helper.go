package message

import (
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/charmbracelet/log"
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
