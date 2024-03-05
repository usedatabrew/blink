package mysql_cdc

type Config struct {
	Host           string `json:"host" yaml:"host"`
	Port           uint16 `json:"port" yaml:"port"`
	Database       string `json:"database" yaml:"database"`
	User           string `json:"user" yaml:"user"`
	Password       string `json:"password" yaml:"password"`
	Flavor         string `json:"flavor" yaml:"flavor"`
	StreamSnapshot bool   `json:"stream_snapshot" yaml:"stream_snapshot"`
}
