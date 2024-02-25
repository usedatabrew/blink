package sources

type SourceDriver string

const (
	PostgresCDC         SourceDriver = "postgres_cdc"
	PostgresIncremental SourceDriver = "postgres_incremental"
	MongoStream         SourceDriver = "mongo_stream"
	WebSockets          SourceDriver = "websocket"
	AirTable            SourceDriver = "airtable"
	Playground          SourceDriver = "playground"
)
