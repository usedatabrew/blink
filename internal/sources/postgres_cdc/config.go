package postgres_cdc

import "github.com/usedatabrew/pglogicalstream"

type Config struct {
	Host           string                           `json:"host" yaml:"host"`
	Port           int                              `json:"port" yaml:"port"`
	Database       string                           `json:"database" yaml:"database"`
	User           string                           `json:"user" yaml:"user"`
	Schema         string                           `json:"schema" yaml:"schema"`
	Password       string                           `json:"password" yaml:"password"`
	TablesSchema   []pglogicalstream.DbTablesSchema `json:"tables_schema" yaml:"tables_schema"`
	SSLRequired    bool                             `json:"ssl_required" yaml:"ssl_required"`
	StreamSnapshot bool                             `json:"stream_snapshot" yaml:"stream_snapshot"`
	SlotName       string                           `json:"slot_name" yaml:"slot_name"`
}
