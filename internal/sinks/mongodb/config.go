package mongodb

type Config struct {
	Uri      string `json:"uri" yaml:"uri"`
	Database string `json:"database" yaml:"database"`
}
