package secret

import (
	"github.com/usedatabrew/blink/internal/secret/secretstorage"
	"gopkg.in/yaml.v3"
)

type SecretsStorageType string

const AwsSecretStorage SecretsStorageType = "aws_secret_storage"
const MockSecretStorage SecretsStorageType = "mock_secret_storage"

type Secrets struct{}

func NewSecrets(storage SecretsStorageType, config any) SecretStorage {
	switch storage {
	case AwsSecretStorage:
		return secretstorage.NewAwsSecretManager(convertSecretsToType[secretstorage.AwsSecretManagerConfig](config))
	case MockSecretStorage:
		return secretstorage.NewMockSecretManager(convertSecretsToType[secretstorage.MockSecretManagerConfig](config))
	default:
		panic("Failed to load secret storage. Check Secret storage type")
	}
}

func convertSecretsToType[T any](config any) T {
	marshaled, err := yaml.Marshal(config)
	if err != nil {
		panic("Failed to convert secrets to type")
	}
	var targetConfig T
	err = yaml.Unmarshal(marshaled, &targetConfig)

	return targetConfig
}
