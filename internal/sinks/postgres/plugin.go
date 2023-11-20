package postgres

import (
	"astro/internal/message"
	"astro/internal/schema"
	"astro/internal/sinks"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
)

type SinkPlugin struct {
	ctx          context.Context
	config       Config
	streamSchema []schema.StreamSchema
	conn         *pgx.Conn
}

func NewPostgresSinkPlugin(config Config, schema []schema.StreamSchema) sinks.DataSink {
	return &SinkPlugin{
		config:       config,
		streamSchema: schema,
	}
}

func (s *SinkPlugin) Connect(context context.Context) error {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		s.config.User,
		s.config.Password,
		s.config.Host,
		s.config.Port,
		s.config.Database,
	)

	conn, err := pgx.Connect(context, connStr)
	s.conn = conn

	return err
}

func (s *SinkPlugin) SetExpectedSchema(schema []schema.StreamSchema) {
	s.streamSchema = schema
	s.generateCreateTableStatements()
}

func (s *SinkPlugin) GetType() sinks.SinkDriver {
	return sinks.PostgresSinkType
}

func (s *SinkPlugin) Write(m message.Message) error {
	//TODO implement me
	panic("implement me")
}

func (s *SinkPlugin) Stop() {
	s.conn.Close(s.ctx)
}

func (s *SinkPlugin) generateCreateTableStatements() {

}
