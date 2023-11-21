package config

import (
	"astro/internal/processors"
	"astro/internal/schema"
	"astro/internal/sinks"
	"astro/internal/sources"
)

type Configuration struct {
	Service    Service     `yaml:"service"`
	Source     Source      `yaml:"source"`
	Processors []Processor `yaml:"processors"`
	Sink       Sink        `yaml:"sink"`
}

type Columns struct {
	Name                string `yaml:"name"`
	DatabrewType        string `yaml:"databrewType"`
	NativeConnectorType string `yaml:"nativeConnectorType"`
	Pk                  bool   `yaml:"pk"`
	Nullable            bool   `yaml:"nullable"`
}

type Service struct {
	ReloadOnRestart    bool                  `yaml:"reload_on_restart"`
	PipelineId         int                   `yaml:"pipeline_id"`
	InfluxEnabled      bool                  `yaml:"enable_influx"`
	EnableETCDRegistry bool                  `yaml:"enable_etcd_registry"`
	ETCD               ETCD                  `yaml:"etcd"`
	Influx             interface{}           `yaml:"influx"`
	StreamSchema       []schema.StreamSchema `yaml:"stream_schema"`
}

type ETCD struct {
	Host string `yaml:"host"`
}

type Source struct {
	Driver sources.SourceDriver `yaml:"driver"`
	Config interface{}          `yaml:"config"`
}

type Processor struct {
	Driver processors.ProcessorDriver `yaml:"driver"`
	Config interface{}                `yaml:"config"`
}

type Sink struct {
	Driver sinks.SinkDriver `yaml:"driver"`
	Config interface{}      `yaml:"config"`
}
