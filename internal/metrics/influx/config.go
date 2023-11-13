package influx

import "gopkg.in/yaml.v3"

type Config struct {
	Url    string `json:"url" yaml:"url"`
	Token  string `json:"token" yaml:"token"`
	Org    string `json:"org" yaml:"org"`
	Bucket string `json:"bucket" yaml:"bucket"`

	GroupName  string `json:"group_name" yaml:"group_name"`
	PipelineId int    `json:"pipeline_id" yaml:"pipeline_id"`
}

func NewConfig(node *yaml.Node) (conf Config, err error) {
	err = node.Decode(&conf)
	if err != nil {
		return conf, err
	}

	return conf, err
}
