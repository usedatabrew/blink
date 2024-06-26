package helper

import (
	"reflect"
	"strconv"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/charmbracelet/log"
	cqtypes "github.com/cloudquery/plugin-sdk/v4/types"
)

func IsPrimitiveType(fieldType string) bool {
	switch fieldType {
	case "Boolean", "Int32", "Int64", "Uint64", "Float64", "Float32", "UUID", "bytea", "Inet", "MAC", "Date32", "String":
		return true
	default:
		return false
	}
}

func MapPlainTypeToArrow(fieldType string) arrow.DataType {
	switch fieldType {
	case "Boolean":
		return arrow.FixedWidthTypes.Boolean
	case "Int16":
		return arrow.PrimitiveTypes.Int16
	case "Int32":
		return arrow.PrimitiveTypes.Int32
	case "Int64":
		return arrow.PrimitiveTypes.Int64
	case "Uint64":
		return arrow.PrimitiveTypes.Uint64
	case "Float64":
		return arrow.PrimitiveTypes.Float64
	case "Float32":
		return arrow.PrimitiveTypes.Float32
	case "UUID":
		return arrow.BinaryTypes.String
	case "bytea":
		return arrow.BinaryTypes.Binary
	case "Inet":
		return arrow.BinaryTypes.String
	case "MAC":
		return arrow.BinaryTypes.String
	case "Date32":
		return arrow.FixedWidthTypes.Date32
	default:
		return arrow.BinaryTypes.String
	}
}

func InferArrowType(value interface{}) arrow.DataType {
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

// GetValue extracts the value at the specified row index from a column using reflection
func GetValue(column arrow.Array, rowIndex int) interface{} {
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

func ArrowToPg10(t arrow.DataType) string {
	switch dt := t.(type) {
	case *arrow.BooleanType:
		return "boolean"
	case *arrow.Int8Type:
		return "smallint"
	case *arrow.Int16Type:
		return "smallint"
	case *arrow.Int32Type:
		return "int"
	case *arrow.Int64Type:
		return "bigint"
	case *arrow.Uint8Type:
		return "smallint"
	case *arrow.Uint16Type:
		return "int"
	case *arrow.Uint32Type:
		return "bigint"
	case *arrow.Uint64Type:
		return "numeric(20,0)"
	case *arrow.Float32Type:
		return "real"
	case *arrow.Float64Type:
		return "double precision"
	case arrow.DecimalType:
		return "numeric(" + strconv.Itoa(int(dt.GetPrecision())) + "," + strconv.Itoa(int(dt.GetScale())) + ")"
	case *arrow.StringType:
		return "text"
	case *arrow.BinaryType:
		return "bytea"
	case *arrow.LargeBinaryType:
		return "bytea"
	case *arrow.TimestampType:
		return "timestamp without time zone"
	case *arrow.Time32Type, *arrow.Time64Type:
		return "time without time zone"
	case *arrow.Date32Type, *arrow.Date64Type:
		return "date"
	case *cqtypes.UUIDType:
		return "uuid"
	case *cqtypes.JSONType:
		return "jsonb"
	case *cqtypes.MACType:
		return "macaddr"
	case *cqtypes.InetType:
		return "inet"
	case *arrow.ListType:
		return ArrowToPg10(dt.Elem()) + "[]"
	case *arrow.FixedSizeListType:
		return ArrowToPg10(dt.Elem()) + "[]"
	case *arrow.LargeListType:
		return ArrowToPg10(dt.Elem()) + "[]"
	case *arrow.MapType:
		return "text"
	default:
		return "text"
	}
}

// ArrowToCockroach converts arrow data type to cockroach data type. CockroachDB lacks support for
// some data types like macaddr and has different aliases for ints.
// See: https://www.cockroachlabs.com/docs/stable/int.html
func ArrowToCockroach(t arrow.DataType) string {
	switch dt := t.(type) {
	case *arrow.BooleanType:
		return "boolean"
	case *arrow.Int8Type:
		return "int2"
	case *arrow.Int16Type:
		return "int2"
	case *arrow.Int32Type:
		return "int8"
	case *arrow.Int64Type:
		return "int8"
	case *arrow.Uint8Type:
		return "int2"
	case *arrow.Uint16Type:
		return "int8"
	case *arrow.Uint32Type:
		return "int8"
	case *arrow.Uint64Type:
		return "numeric(20,0)"
	case *arrow.Float32Type:
		return "real"
	case *arrow.Float64Type:
		return "double precision"
	case arrow.DecimalType:
		return "numeric(" + strconv.Itoa(int(dt.GetPrecision())) + "," + strconv.Itoa(int(dt.GetScale())) + ")"
	case *arrow.StringType:
		return "text"
	case *arrow.BinaryType:
		return "bytea"
	case *arrow.LargeBinaryType:
		return "bytea"
	case *arrow.TimestampType:
		return "timestamp without time zone"
	case *arrow.Time32Type, *arrow.Time64Type:
		return "time without time zone"
	case *arrow.Date32Type, *arrow.Date64Type:
		return "date"
	case *cqtypes.UUIDType:
		return "uuid"
	case *cqtypes.JSONType:
		return "jsonb"
	case *cqtypes.MACType:
		return "text"
	case *cqtypes.InetType:
		return "inet"
	case *arrow.ListType:
		return ArrowToCockroach(dt.Elem()) + "[]"
	case *arrow.FixedSizeListType:
		return ArrowToCockroach(dt.Elem()) + "[]"
	case *arrow.LargeListType:
		return ArrowToCockroach(dt.Elem()) + "[]"
	case *arrow.MapType:
		return "text"
	default:
		return "text"
	}
}

func ArrowToClickHouse(t arrow.DataType) string {
	switch t.(type) {
	case *arrow.BooleanType:
		return "bool"
	case *arrow.Uint8Type:
		return "UInt8"
	case *arrow.Uint16Type:
		return "UInt16"
	case *arrow.Uint32Type:
		return "UInt32"
	case *arrow.Uint64Type:
		return "UInt64"
	case *arrow.Int8Type:
		return "Int8"
	case *arrow.Int16Type:
		return "Int16"
	case *arrow.Int32Type:
		return "Int32"
	case *arrow.Int64Type:
		return "Int64"
	case *arrow.Float32Type:
		return "Float32"
	case *arrow.Float64Type:
		return "Float64"
	case arrow.DecimalType:
		return "Decimal"
	case *arrow.StringType:
		return "String"
	case *arrow.Date32Type:
		return "Date32"
	case *arrow.Date64Type:
		return "Date64"
	case *arrow.TimestampType:
		return "DateTime"
	case *cqtypes.JSONType:
		return "JSON"
	default:
		return "String"
	}
}
