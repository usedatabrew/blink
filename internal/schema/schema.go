package schema

import "github.com/apache/arrow/go/v14/arrow"

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
	Columns             []Column `yaml:"columns"`
}

func (s *StreamSchema) AsArrow() *arrow.Schema {

	return nil
}

func remove[T any](slice []T, s int) []T {
	return append(slice[:s], slice[s+1:]...)
}
