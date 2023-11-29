package stream

import (
	"blink/config"
	"bytes"
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

func replaceEnvVariables(config []byte, lookupFunc func(string) (string, bool)) []byte {
	replaced := envRegex.ReplaceAllFunc(config, func(content []byte) []byte {
		var value string

		if len(content) > 3 {
			if colonIndex := bytes.IndexByte(content, ':'); colonIndex == -1 {
				varName := string(content[2 : len(content)-1])
				value, _ = lookupFunc(varName)
			} else {
				targetVar := content[2:colonIndex]
				defaultVal := content[colonIndex+1 : len(content)-1]
				value, _ := lookupFunc(string(targetVar))
				if value == "" {
					value = string(defaultVal)
				}
			}
			value = strings.ReplaceAll(value, []byte("$$$1"))
		}

		return []byte(value)
	})

	return replaced
}
