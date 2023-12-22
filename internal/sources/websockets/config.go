package websockets

type Config struct {
	Url     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}
