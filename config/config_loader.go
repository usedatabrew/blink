package config

import (
	"os"
	"regexp"
	"strings"

	"github.com/usedatabrew/blink/internal/logger"
	"github.com/usedatabrew/blink/internal/secret"

	"gopkg.in/yaml.v3"
)

var (
	envRegex        = regexp.MustCompile(`\${[0-9A-Za-z_.]+(:((\${[^}]+})|[^}])*)?}`)
	escapedEnvRegex = regexp.MustCompile(`\${({[0-9A-Za-z_.]+(:((\${[^}]+})|[^}])*)?})}`)
	secretsRegex    = regexp.MustCompile(`#{secret\.[^}]+}`)
)

func ReadInitConfigFromYaml(configBytes []byte) (Configuration, error) {
	replacedEnvVariables := replaceEnvVariables(configBytes, os.LookupEnv)
	resolvedSecrets := resolveSecrets(replacedEnvVariables)

	conf := Configuration{}
	err := yaml.Unmarshal(resolvedSecrets, &conf)

	for _, ss := range conf.Source.StreamSchema {
		ss.SortColumnsAsc()
	}

	ValidateConfigSchema(conf)
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

func resolveSecrets(config []byte) []byte {
	// we read config here as we need to get access to secrets settings
	conf := Configuration{}
	err := yaml.Unmarshal(config, &conf)
	if err != nil {
		panic(err)
	}

	// Skip replacing if no secrets provided
	if conf.Secrets.StorageType == "" {
		return config
	}

	secretStorage := secret.NewSecrets(conf.Secrets.StorageType, conf.Secrets.Config)

	replaced := secretsRegex.ReplaceAllFunc(config, func(content []byte) []byte {
		var value string
		if len(content) > 4 {
			varName := string(content[2 : len(content)-1])
			secretValue, err := secretStorage.Retrieve(varName)
			if err != nil {
				logger.GetInstance().Fatalf("Failed to resolve secret value. Error: %s", err.Error())
			}
			value = secretValue
			value = strings.ReplaceAll(value, "\n", "\\n")
		}
		return []byte(value)
	})
	replaced = escapedEnvRegex.ReplaceAll(replaced, []byte("$$$1"))

	return replaced
}
