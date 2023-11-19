package schema

import (
	"github.com/apache/arrow/go/v14/arrow"
)

type StreamSchema struct {
	StreamName string   `yaml:"stream"`
	Columns    []Column `yaml:"columns"`
}

type Column struct {
	Name                string `yaml:"name"`
	DatabrewType        string `yaml:"databrewType"`
	NativeConnectorType string `yaml:"nativeConnectorType"`
	PK                  bool   `yaml:"pk"`
	Nullable            bool   `yaml:"nullable"`
}

type StreamSchemaObj struct {
	streamSchema []StreamSchema
}

func NewStreamSchemaObj(s []StreamSchema) *StreamSchemaObj {
	return &StreamSchemaObj{streamSchema: s}
}

func (s StreamSchemaObj) AddField(streamName, name string, fieldType arrow.DataType) {
	for idx, stream := range s.streamSchema {
		if stream.StreamName == streamName {
			arrowColumn := Column{
				Name:                name,
				DatabrewType:        fieldType.String(),
				NativeConnectorType: "String",
				PK:                  false,
				Nullable:            true,
			}
			stream.Columns = append(stream.Columns, arrowColumn)
			s.streamSchema[idx] = stream
		}
	}
}

// TODO:: add columns removal
func (s StreamSchemaObj) RemoveField(name string) {
	//for idx, col := range s.Columns {
	//	if col.Name == name {
	//		s.Columns = remove(s.Columns, idx)
	//	}
	//}
}

func remove(slice []Column, s int) []Column {
	return append(slice[:s], slice[s+1:]...)
}
