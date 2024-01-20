package postgres

import (
	"context"
	"fmt"
	"github.com/charmbracelet/log"
	"github.com/jackc/pgx/v5"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sinks"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/message"
	"sync"
	"time"
)

type SinkPlugin struct {
	appctx                *stream_context.Context
	config                Config
	streamSchema          []schema.StreamSchema
	conn                  *pgx.Conn
	logger                *log.Logger
	rowStatements         map[string]map[message.Event]string
	pkColumnNamesByStream map[string]string
	mutex                 sync.Mutex
	messagesBuffer        []*message.Message
	snapshotMaxBufferSize int
	prevEvent             message.Event
	prevSnapshotStream    string
	snapshotTicker        *time.Timer
}

func NewPostgresSinkPlugin(config Config, schema []schema.StreamSchema, appctx *stream_context.Context) sinks.DataSink {
	return &SinkPlugin{
		config:                config,
		appctx:                appctx,
		streamSchema:          schema,
		logger:                log.WithPrefix("PostgreSQL Sink"),
		messagesBuffer:        []*message.Message{},
		snapshotMaxBufferSize: 5000,
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

func (s *SinkPlugin) Write(m *message.Message) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// for snapshot event we have to perform inserts in bulk using COPY command
	// to achieve higher insert efficiency

	if m.GetEvent() == message.Snapshot && s.prevEvent == message.Snapshot && s.prevSnapshotStream != m.GetStream() {
		// we have to drain snapshot message for prev stream
		// before we can process snapshot for another stream
		s.logger.Info("Changed stream for snapshot. Draining message buffer to continue")
		err := s.writeSnapshotBatch()
		if err != nil {
			return err
		}
	}

	s.prevSnapshotStream = m.GetStream()
	if m.GetEvent() == message.Snapshot {
		s.messagesBuffer = append(s.messagesBuffer, m)

		if len(s.messagesBuffer) >= s.snapshotMaxBufferSize {
			err := s.writeSnapshotBatch()
			if err != nil {
				return err
			}

			s.messagesBuffer = []*message.Message{}
			return nil
		} else if s.snapshotTicker == nil {
			// Start a timer if not already running
			s.snapshotTicker = time.AfterFunc(time.Second, func() {
				s.mutex.Lock()
				defer s.mutex.Unlock()
				err := s.writeSnapshotBatch()
				if err != nil {
					s.logger.Fatal(err)
					panic("Failed to write snapshot batch")
				}
				s.messagesBuffer = []*message.Message{}
				s.snapshotTicker.Stop()
				s.snapshotTicker = nil
			})
			return nil
		} else {
			return nil
		}
	}
	// means we finished snapshot streaming. Here we must ensure that we flushed all the
	// messages from snapshot before we start pushing rest of the messages
	if s.prevEvent == message.Snapshot && s.prevEvent != m.GetEvent() {
		s.logger.Info("Snapshot streaming finished. Draining message buffer to continue")
		err := s.writeSnapshotBatch()
		if err != nil {
			return err
		}
	}
	s.prevEvent = m.GetEvent()

	tableStatement := s.rowStatements[s.config.StreamPrefix+m.GetStream()][m.GetEvent()]
	var colValues []interface{}
	var pkColValue interface{}
	// we apply different flow for deletion requests
	// since we don't have to bind all the params, we are interested only in PK
	s.logger.Info("Applying operation", "op", m.GetEvent(), "stream", m.GetStream())
	if m.GetEvent() == message.Delete {
		pkColValue = m.Data.AccessProperty(s.pkColumnNamesByStream[m.GetStream()])
		if pkColValue != nil {
			colValues = append(colValues, pkColValue)
		}
		_, err := s.conn.Exec(s.appctx.GetContext(), tableStatement, colValues...)
		if err != nil {
			return err
		}
	} else if m.GetEvent() == message.Insert {
		for _, ss := range s.streamSchema {
			if ss.StreamName == m.Stream {
				for _, col := range getColumnNamesSorted(ss.Columns) {
					colValues = append(colValues, m.Data.AccessProperty(col))
				}
			}
		}

		_, err := s.conn.Exec(s.appctx.GetContext(), tableStatement, colValues...)
		if err != nil {
			return err
		}
	} else {
		// else is for update statements
		pkColName := s.pkColumnNamesByStream[s.prevSnapshotStream]
		if pkColName == "" {
			s.logger.Debug("Update statement is not supported for PG without PK")
			return nil
		}

		for _, ss := range s.streamSchema {
			if ss.StreamName == m.Stream {
				for _, col := range getColumnNamesSorted(ss.Columns) {
					if col != pkColName {
						colValues = append(colValues, m.Data.AccessProperty(col))
					}
				}

				colValues = append(colValues, m.Data.AccessProperty(pkColName))
			}
		}

		_, err := s.conn.Exec(s.appctx.GetContext(), tableStatement, colValues...)
		if err != nil {
			return err
		}
	}

	return nil
}

// writeSnapshotBatch is used only when we restore snapshot / to back-filling
// from the sink. in order to writeSnapshotBatch to be supported - source plugin
// must emit "snapshot" event instead of stream event
func (s *SinkPlugin) writeSnapshotBatch() error {
	var messagesToInsert [][]interface{}
	var colNames []string

	if len(s.messagesBuffer) == 0 {
		return nil
	}

	// extract column names
	for _, ss := range s.streamSchema {
		if ss.StreamName == s.messagesBuffer[0].Stream {
			for _, col := range ss.Columns {
				colNames = append(colNames, col.Name)
			}
		}
	}

	for _, bufMessage := range s.messagesBuffer {
		// then we extract all the column values that we will use
		// in CopyFromRows statement
		var colValues []interface{}
		for _, ss := range s.streamSchema {
			if ss.StreamName == bufMessage.Stream {
				for _, col := range ss.Columns {
					colValues = append(colValues, bufMessage.Data.AccessProperty(col.Name))
				}
			}
		}

		messagesToInsert = append(messagesToInsert, colValues)
	}

	_, err := s.conn.CopyFrom(context.TODO(), pgx.Identifier{s.config.StreamPrefix + s.prevSnapshotStream}, colNames, pgx.CopyFromRows(messagesToInsert))

	return err
}

func (s *SinkPlugin) Stop() {
	s.conn.Close(s.appctx.GetContext())
}

func (s *SinkPlugin) createInitStatements() {
	var dbCreateTableStatements []string
	var rowStatements = make(map[string]map[message.Event]string)
	var pkColumnNames = make(map[string]string)

	for _, stream := range s.streamSchema {
		stream.StreamName = s.config.StreamPrefix + stream.StreamName
		dbCreateTableStatements = append(dbCreateTableStatements, generateCreateTableStatement(stream.StreamName, stream.Columns))

		insertStatement := generateBatchInsertStatement(stream)
		updateStatement := generateBatchUpdateStatement(stream)
		deleteStatement := generateBatchDeleteStatement(stream)
		rowStatements[stream.StreamName] = map[message.Event]string{
			message.Delete:   deleteStatement,
			message.Update:   updateStatement,
			message.Snapshot: insertStatement,
			message.Insert:   insertStatement,
		}

		for _, col := range stream.Columns {
			if col.PK {
				pkColumnNames[stream.StreamName] = col.Name
			}
		}
	}

	s.pkColumnNamesByStream = pkColumnNames
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
}
