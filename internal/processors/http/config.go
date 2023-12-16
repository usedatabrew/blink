package http

type Config struct {
	Source      string `json:"source" yaml:"source"`
	TargetField string `json:"target_field" yaml:"target_field"`
	StreamName  string `json:"stream" yaml:"stream"`
	Endpoint    string `json:"endpoint" yaml:"endpoint"`
	Method      string `json:"method" yaml:"method"`
	Auth        Auth   `json:"auth" yaml:"auth"`
}

type Auth struct {
	Headers           map[string]string `json:"headers" yaml:"headers"`
	BasicAuthUser     string            `json:"basic_auth_user" yaml:"basic_auth_user"`
	BasicAuthPassword string            `json:"basic_auth_password" yaml:"basic_auth_password"`
}
