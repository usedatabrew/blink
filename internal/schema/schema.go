package schema

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
