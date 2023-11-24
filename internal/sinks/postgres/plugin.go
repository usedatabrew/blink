package postgres

import (
	"astro/internal/message"
	"astro/internal/schema"
	"astro/internal/sinks"
	"astro/internal/stream_context"
	"context"
	"fmt"
	"github.com/charmbracelet/log"
	"github.com/jackc/pgx/v5"
)

type SinkPlugin struct {
	appctx        *stream_context.Context
	config        Config
	streamSchema  []schema.StreamSchema
	conn          *pgx.Conn
	logger        *log.Logger
	rowStatements map[string]map[string]string
}

func NewPostgresSinkPlugin(config Config, schema []schema.StreamSchema, appctx *stream_context.Context) sinks.DataSink {
	return &SinkPlugin{
		config:       config,
		appctx:       appctx,
		streamSchema: schema,
		logger:       log.WithPrefix("PostgreSQL Sink"),
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
	s.createInitStatements()
}

func (s *SinkPlugin) GetType() sinks.SinkDriver {
	return sinks.PostgresSinkType
}

func (s *SinkPlugin) Write(m message.Message) error {
	tableStatement := s.rowStatements[m.GetStream()][m.GetEvent()]
	s.logger.Info("Perform SQL operation", "op", tableStatement)
	var colValues []interface{}
	for idx := range m.Data.Columns() {
		colValues = append(colValues, message.GetValue(m.Data.Column(idx), 0))
	}
	_, err := s.conn.Exec(s.appctx.GetContext(), tableStatement, colValues...)

	return err
}

func (s *SinkPlugin) Stop() {
	s.conn.Close(s.appctx.GetContext())
}

func (s *SinkPlugin) createInitStatements() {
	var dbCreateTableStatements []string
	var rowStatements = make(map[string]map[string]string)

	for _, stream := range s.streamSchema {
		dbCreateTableStatements = append(dbCreateTableStatements, generateCreateTableStatement(stream.StreamName, stream.Columns))

		insertStatement := generateBatchInsertStatement(stream)
		updateStatement := generateBatchUpdateStatement(stream)
		deleteStatement := generateBatchDeleteStatement(stream)
		rowStatements[stream.StreamName] = map[string]string{
			"delete": deleteStatement,
			"update": updateStatement,
			"insert": insertStatement,
		}
	}
	s.rowStatements = rowStatements

	s.logger.Info("Generated init statements to create table for the sink database", "statements", dbCreateTableStatements)
	tx, err := s.conn.Begin(s.appctx.GetContext())
	defer tx.Rollback(s.appctx.GetContext())
	if err != nil {
		s.logger.Fatal("Failed to init transaction to create tables for sink", "err", err)
	}

	for idx, stream := range s.streamSchema {
		s.logger.Info("Creating table for stream", "stream", stream.StreamName)
		_, err := tx.Exec(s.appctx.GetContext(), dbCreateTableStatements[idx])
		if err != nil {
			s.logger.Fatal("Failed to create table for stream", "stream", stream.StreamName, "error", err)
			tx.Rollback(s.appctx.GetContext())
		}
	}

	tx.Commit(s.appctx.GetContext())
	//s.logger.Fatal("Intentionally exited")
}
