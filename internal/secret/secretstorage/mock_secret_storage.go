package secretstorage

import "fmt"

type MockSecretManagerConfig struct {
	SomeSetting string `yaml:"some_setting"`
}

type MockSecretManager struct {
}

func NewMockSecretManager(config MockSecretManagerConfig) *MockSecretManager {
	msm := &MockSecretManager{}

	return msm
}

func (a *MockSecretManager) Retrieve(key string) (string, error) {
	return fmt.Sprintf("value_%s", key), nil
}
