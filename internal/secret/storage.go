package secret

type SecretStorage interface {
	Retrieve(secretKey string) (string, error)
}
