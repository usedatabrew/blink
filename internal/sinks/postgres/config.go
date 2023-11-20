package postgres

type Config struct {
	Host        string `json:"host" yaml:"host"`
	Port        int    `json:"port" yaml:"port"`
	Database    string `json:"database" yaml:"database"`
	User        string `json:"user" yaml:"user"`
	Schema      string `json:"schema" yaml:"schema"`
	Password    string `json:"password" yaml:"password"`
	SSLRequired bool   `json:"ssl_required" yaml:"ssl_required"`
}
