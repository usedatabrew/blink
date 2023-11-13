package config

import (
	"astro/internal/schema"
	"astro/internal/sinks"
	"astro/internal/sources"
)

type Configuration struct {
	Service Service `yaml:"service"`
	Source  Source  `yaml:"source"`
	Sink    Sink    `yaml:"sink"`
}
type Columns struct {
	Name                string `yaml:"name"`
	DatabrewType        string `yaml:"databrewType"`
	NativeConnectorType string `yaml:"nativeConnectorType"`
	Pk                  bool   `yaml:"pk"`
	Nullable            bool   `yaml:"nullable"`
}
type Service struct {
	EnableInflux    bool                  `yaml:"enable_influx"`
	ReloadOnRestart bool                  `yaml:"reload_on_restart"`
	StreamSchema    []schema.StreamSchema `yaml:"stream_schema"`
}

type Source struct {
	Driver sources.SourceDriver `yaml:"driver"`
	Config interface{}          `yaml:"config"`
}

type Sink struct {
	Driver sinks.DataSink `yaml:"driver"`
	Config interface{}    `yaml:"config"`
}
