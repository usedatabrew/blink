package sources

type SourceDriver string

const (
	PostgresCDC SourceDriver = "postgres_cdc"
	MongoStream SourceDriver = "mongo_stream"
	WebSockets  SourceDriver = "websocket"
)
