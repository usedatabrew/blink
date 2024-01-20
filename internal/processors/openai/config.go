package openai

type Config struct {
	ApiKey         string `json:"api_key" yaml:"api_key"`
	SourceField    string `json:"source_field" yaml:"source_field"`
	TargetField    string `json:"target_field" yaml:"target_field"`
	Model          string `json:"model" yaml:"model"`
	Prompt         string `json:"prompt" yaml:"prompt"`
	StreamName     string `json:"stream_name" yaml:"stream_name"`
	LimitPerMinute int64  `json:"limit_per_minute" yaml:"limit_per_minute"`
}
