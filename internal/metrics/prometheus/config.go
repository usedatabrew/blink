package prometheus

import "gopkg.in/yaml.v3"

type Config struct {
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
