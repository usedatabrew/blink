package config

type Configuration struct {
	Service ServiceConfig `yaml:"stream"`
	Input   InputConfig   `yaml:"input"`
	Target  TargetConfig  `yaml:"target"`
}

type ServiceConfig struct {
	EnableInflux    bool `yaml:"enable_influx"`
	ReloadOnRestart bool `yaml:"reload_on_restart"`
}

type InputConfig struct {
	Label  string      `yaml:"label"`
	Schema []Stream    `yaml:"schema"`
	Config interface{} `yaml:"config"`
}

type Stream struct {
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

type TargetConfig struct {
	Plugin string      `yaml:"plugin"`
	Config interface{} `yaml:"config"`
}
