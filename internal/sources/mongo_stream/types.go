package mongo_stream

import "github.com/apache/arrow/go/v14/arrow"

func MapPlainTypeToArrow(fieldType string) arrow.DataType {
	switch fieldType {
	default:
		return arrow.BinaryTypes.String
	}
}
