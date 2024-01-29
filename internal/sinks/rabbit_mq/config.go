package rabbit_mq

type Config struct {
	Url          string `json:"url" yaml:"url"`
	ExchangeName string `json:"exchange_name" yaml:"exchange_name"`
	RoutingKey   string `json:"routing_key" yaml:"routing_key"`
}
