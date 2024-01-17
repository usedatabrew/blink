package redis

type Config struct {
	RedisAddr     string `json:"redis_addr" yaml:"redis_addr"`
	RedisPassword string `json:"redis_password" yaml:"redis_password"`
	// CustomNamespace can't be used alongside with NamespaceByStream, only one of the
	// Options should be provided.
	// In case provided both - namespace by stream will be used
	CustomNamespace   string `json:"custom_namespace" yaml:"custom_namespace"`
	NamespaceByStream bool   `json:"namespace_by_stream" yaml:"namespace_by_stream"`
	KeyPrefix         string `json:"key_prefix" yaml:"key_prefix"`
	SetWithTTL        int64  `json:"set_with_ttl" yaml:"set_with_ttl"`
}
