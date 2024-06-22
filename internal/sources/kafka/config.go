package kafka

type Config struct {
	Brokers       []string `json:"brokers" yaml:"brokers"`
	Sasl          bool     `json:"sasl" yaml:"sasl"`
	SaslPassword  string   `json:"sasl_password" yaml:"sasl_password"`
	SaslUser      string   `json:"sasl_user" yaml:"sasl_user"`
	SaslMechanism string   `json:"sasl_mechanism" yaml:"sasl_mechanism"`
	ConsumerGroup string   `json:"consumer_group" yaml:"consumer_group"`
}
