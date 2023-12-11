package openai

type Config struct {
	ApiKey      string `json:"api_key" yaml:"api_key"`
	SourceField string `json:"source_field" yaml:"source_field"`
	TargetField string `json:"target_field" yaml:"target_field"`
	StreamName  string `json:"stream_name" yaml:"stream_name"`
}
