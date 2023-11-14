package mongo_stream

type Config struct {
	Uri            string `json:"uri" yaml:"uri"`
	Database       string `json:"database" yaml:"database"`
	Collection     string `json:"collection" yaml:"collection"`
	StreamSnapshot bool   `json:"stream_snapshot" yaml:"stream_snapshot"`
}
