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
	streamSchemaVersions map[int][]StreamSchema
	lastVersion          int
}

func NewStreamSchemaObj(s []StreamSchema) *StreamSchemaObj {
	obj := &StreamSchemaObj{
		streamSchemaVersions: map[int][]StreamSchema{},
		lastVersion:          0,
	}
	obj.streamSchemaVersions[obj.lastVersion] = s
	return obj
}

func (s *StreamSchemaObj) GetLatestSchema() []StreamSchema {
	return s.streamSchemaVersions[s.lastVersion]
}

func (s *StreamSchemaObj) AddField(streamName, name string, fieldType arrow.DataType, driverType string) {
	streamSchema := s.streamSchemaVersions[s.lastVersion]
	var streamSchemaCopy = make([]StreamSchema, len(streamSchema))
	copy(streamSchemaCopy, streamSchema)
	for idx, stream := range streamSchemaCopy {
		if stream.StreamName == streamName {
			arrowColumn := Column{
				Name:                name,
				DatabrewType:        fieldType.String(),
				NativeConnectorType: driverType,
				PK:                  false,
				Nullable:            true,
			}

			stream.Columns = append(stream.Columns, arrowColumn)
			streamSchemaCopy[idx] = stream
		}
	}

	s.lastVersion += 1
	s.streamSchemaVersions[s.lastVersion] = streamSchemaCopy
}

// TODO:: add columns removal
func (s *StreamSchemaObj) RemoveField(name string) {
	//for idx, col := range s.Columns {
	//	if col.Name == name {
	//		s.Columns = remove(s.Columns, idx)
	//	}
	//}
}

func remove(slice []Column, s int) []Column {
	return append(slice[:s], slice[s+1:]...)
}
