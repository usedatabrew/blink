package websockets

type Config struct {
	Url     string            `json:"url" yaml:"url"`
	Headers map[string]string `json:"headers" yaml:"headers"`
}
