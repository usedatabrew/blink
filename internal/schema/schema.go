package schema

import (
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/barkimedes/go-deepcopy"
	"slices"
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
	var streamSchemaCopy = s.getLastSchemaDeepCopy()
	for idx, stream := range streamSchemaCopy {
		if stream.StreamName == streamName {
			arrowColumn := Column{
				Name:                name,
				DatabrewType:        fieldType.String(),
				NativeConnectorType: driverType,
				PK:                  false,
				Nullable:            true,
			}

			streamSchemaCopy[idx].Columns = append(stream.Columns, arrowColumn)
		}
	}

	s.lastVersion += 1
	s.streamSchemaVersions[s.lastVersion] = streamSchemaCopy
}

func (s *StreamSchemaObj) RemoveField(streamName, columnName string) {
	var streamSchemaCopy = s.getLastSchemaDeepCopy()
	for streamIndex, stream := range streamSchemaCopy {
		if stream.StreamName == streamName {
			for colIdx, column := range stream.Columns {
				if column.Name == columnName {
					streamSchemaCopy[streamIndex].Columns = remove(stream.Columns, colIdx)
				}
			}
		}
	}

	s.lastVersion += 1
	s.streamSchemaVersions[s.lastVersion] = streamSchemaCopy
}

func (s *StreamSchemaObj) RemoveFields(streamName string, columnNames []string) {
	var streamSchemaCopy = s.getLastSchemaDeepCopy()
	for streamIndex, stream := range streamSchemaCopy {
		if stream.StreamName == streamName {
			for colIdx, column := range stream.Columns {
				if idx := slices.Index(columnNames, column.Name); idx != -1 {
					streamSchemaCopy[streamIndex].Columns = remove(stream.Columns, colIdx)
					columnNames = remove(columnNames, idx)
				}
			}
		}
	}

	s.lastVersion += 1
	s.streamSchemaVersions[s.lastVersion] = streamSchemaCopy
}

func (s *StreamSchemaObj) getLastSchemaDeepCopy() []StreamSchema {
	streamSchema := s.streamSchemaVersions[s.lastVersion]
	//var streamSchemaCopy = make([]StreamSchema, len(streamSchema))
	streamSchemaCopy, err := deepcopy.Anything(streamSchema)
	if err != nil {
		panic(err)
	}
	return streamSchemaCopy.([]StreamSchema)
}

func remove[T any](slice []T, s int) []T {
	return append(slice[:s], slice[s+1:]...)
}
