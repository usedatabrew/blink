package config

import (
	"github.com/usedatabrew/blink/internal/processors"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sinks"
	"github.com/usedatabrew/blink/internal/sources"
)

type Configuration struct {
	Service    Service     `yaml:"service" validate:"required"`
	Source     Source      `yaml:"source" validate:"required"`
	Processors []Processor `yaml:"processors"`
	Sink       Sink        `yaml:"sink" validate:"required"`
}

type Columns struct {
	Name                string `yaml:"name"`
	DatabrewType        string `yaml:"databrewType"`
	NativeConnectorType string `yaml:"nativeConnectorType"`
	Pk                  bool   `yaml:"pk"`
	Nullable            bool   `yaml:"nullable"`
}

type Service struct {
	ReloadOnRestart    bool        `yaml:"reload_on_restart"`
	PipelineId         int         `yaml:"pipeline_id" validate:"required"`
	InfluxEnabled      bool        `yaml:"enable_influx"`
	EnableETCDRegistry bool        `yaml:"enable_etcd_registry"`
	ETCD               *ETCD       `yaml:"etcd"`
	Influx             interface{} `yaml:"influx"`
}

type ETCD struct {
	Host string `yaml:"host" validate:"required,http_url"`
}

type Source struct {
	Driver       sources.SourceDriver  `yaml:"driver"`
	Config       interface{}           `yaml:"config"`
	StreamSchema []schema.StreamSchema `yaml:"stream_schema" validate:"required"`
}

type Processor struct {
	Driver processors.ProcessorDriver `yaml:"driver"`
	Config interface{}                `yaml:"config"`
}

type Sink struct {
	Driver sinks.SinkDriver `yaml:"driver"`
	Config interface{}      `yaml:"config"`
}
