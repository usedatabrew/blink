package mongo_stream

type Config struct {
	Uri            string `json:"uri" yaml:"uri"`
	Database       string `json:"database" yaml:"database"`
	StreamSnapshot bool   `json:"stream_snapshot" yaml:"stream_snapshot"`
}
