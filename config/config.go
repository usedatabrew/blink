package config

import (
	"astro/internal/schema"
	"astro/internal/sinks"
	"astro/internal/sources"
)

type Configuration struct {
	Service ServiceConfig `yaml:"stream"`
	Input   InputConfig   `yaml:"input"`
	Target  TargetConfig  `yaml:"target"`
}

type ServiceConfig struct {
	EnableInflux    bool                  `yaml:"enable_influx"`
	ReloadOnRestart bool                  `yaml:"reload_on_restart"`
	StreamSchema    []schema.StreamSchema `yaml:"stream_schema"`
}

type InputConfig struct {
	Driver sources.SourceDriver `yaml:"driver"`
	Config interface{}          `yaml:"config"`
}

type TargetConfig struct {
	Driver sinks.SinkDriver `yaml:"driver"`
	Config interface{}      `yaml:"config"`
}
