package kafka

type Config struct {
	Brokers           []string `json:"brokers" yaml:"brokers"`
	Sasl              bool     `json:"sasl" yaml:"sasl"`
	SaslPassword      string   `json:"sasl_password" yaml:"sasl_password"`
	SaslUser          string   `json:"sasl_user" yaml:"sasl_user"`
	SaslMechanism     string   `json:"sasl_mechanism" yaml:"sasl_mechanism"`
	BindTopicToStream bool     `json:"bind_topic_to_stream" yaml:"bind_topic_to_stream"`
	TopicName         string   `json:"topic_name" yaml:"topic_name"`
}
