package stream

import (
	"astro/config"
	"gopkg.in/yaml.v3"
)

func ReadInitConfigFromYaml(configBytes []byte) (config.Configuration, error) {
	conf := config.Configuration{}
	err := yaml.Unmarshal(configBytes, &conf)
	config.ValidateConfigSchema(conf)
	return conf, err
}

func ReadDriverConfig[T any](driverConfig interface{}, targetConfig T) (T, error) {
	marshaled, err := yaml.Marshal(driverConfig)
	if err != nil {
		return targetConfig, err
	}
	err = yaml.Unmarshal(marshaled, &targetConfig)

	return targetConfig, err
}
