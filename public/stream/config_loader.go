package stream

import (
	"github.com/usedatabrew/blink/config"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

var (
	envRegex        = regexp.MustCompile(`\${[0-9A-Za-z_.]+(:((\${[^}]+})|[^}])*)?}`)
	escapedEnvRegex = regexp.MustCompile(`\${({[0-9A-Za-z_.]+(:((\${[^}]+})|[^}])*)?})}`)
)

func ReadInitConfigFromYaml(configBytes []byte) (config.Configuration, error) {
	replacedBytes := replaceEnvVariables(configBytes, os.LookupEnv)

	conf := config.Configuration{}
	err := yaml.Unmarshal(replacedBytes, &conf)

	for _, ss := range conf.Source.StreamSchema {
		ss.SortColumnsAsc()
	}

	config.ValidateConfigSchema(conf)
	return conf, err
}

func ReadDriverConfig[T any](driverConfig interface{}, targetConfig T) (T, error) {
	marshaled, err := yaml.Marshal(driverConfig)
	if err != nil {
		return targetConfig, err
	}

	replacedBytes := replaceEnvVariables(marshaled, os.LookupEnv)

	err = yaml.Unmarshal(replacedBytes, &targetConfig)

	return targetConfig, err
}

func replaceEnvVariables(config []byte, lookupFn func(string) (string, bool)) []byte {
	replaced := envRegex.ReplaceAllFunc(config, func(content []byte) []byte {
		var value string
		if len(content) > 3 {
			varName := string(content[2 : len(content)-1])
			value, _ = lookupFn(varName)
			value = strings.ReplaceAll(value, "\n", "\\n")
		}
		return []byte(value)
	})
	replaced = escapedEnvRegex.ReplaceAll(replaced, []byte("$$$1"))

	return replaced
}
