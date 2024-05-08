package secretstorage

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
)

type AwsSecretManagerConfig struct {
	SecretKey   string `yaml:"aws_secret_key"`
	SecretKeyID string `yaml:"aws_secret_key_id"`
	Region      string `yaml:"aws_region"`
}

type AwsSecretManager struct {
	secretsmanager *secretsmanager.SecretsManager
}

func NewAwsSecretManager(config AwsSecretManagerConfig) *AwsSecretManager {
	awsSession, err := session.NewSession(&aws.Config{
		Region:      aws.String(config.Region),
		Credentials: credentials.NewStaticCredentials(config.SecretKeyID, config.SecretKey, ""),
	})

	if err != nil {
		panic(err)
	}

	asm := &AwsSecretManager{
		secretsmanager: secretsmanager.New(awsSession),
	}

	return asm
}

func (a *AwsSecretManager) Retrieve(key string) (string, error) {
	fmt.Println("Rertienving secret from AWS Secret Manager", key)
	v, err := a.secretsmanager.GetSecretValue(&secretsmanager.GetSecretValueInput{
		SecretId: aws.String(key),
	})

	if err != nil {
		return "", err
	}

	return *v.SecretString, nil
}
