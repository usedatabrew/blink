package nats

type Config struct {
	Url      string `json:"url" yaml:"url"`
	Subject  string `json:"subject" yaml:"subject"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}
