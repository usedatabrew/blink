package clickhouse

type Config struct {
	Host     string `json:"host" yaml:"host"`
	Port     int    `json:"port" yaml:"port"`
	Database string `json:"database" yaml:"database"`
	User     string `json:"user" yaml:"user"`
	Password string `json:"password" yaml:"password"`
}
