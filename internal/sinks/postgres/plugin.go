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

	if m.GetEvent() == "snapshot" && s.prevEvent == "snapshot" && s.prevSnapshotStream != m.GetStream() {
		// we have to drain snapshot message for prev stream
		// before we can process snapshot for another stream
		s.logger.Info("Changed stream for snapshot. Draining message buffer to continue")
		err := s.writeSnapshotBatch()
		if err != nil {
			return err
		}
	}

	s.prevSnapshotStream = m.GetStream()
	if m.GetEvent() == "snapshot" {
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
	if s.prevEvent == "snapshot" && s.prevEvent != m.GetEvent() {
		s.logger.Info("Snapshot streaming finished. Draining message buffer to continue")
		err := s.writeSnapshotBatch()
		if err != nil {
			return err
		}
	}
	s.prevEvent = m.GetEvent()

	tableStatement := s.rowStatements[m.GetStream()][m.GetEvent()]
	var colValues []interface{}
	var pkColValue interface{}
	// we apply different flow for deletion requests
	// since we don't have to bind all the params, we are interested only in PK
	s.logger.Info("Applying operation", "op", m.GetEvent(), "stream", m.GetStream())
	if m.GetEvent() == "delete" {
		pkColumnValue := m.Data.AccessProperty(s.pkColumnNamesByStream[m.GetStream()])
		if pkColumnValue != nil {
			colValues = append(colValues, pkColValue)
		}
		_, err := s.conn.Exec(s.appctx.GetContext(), tableStatement, colValues...)
		if err != nil {
			return err
		}
	} else {
		pkColName := s.pkColumnNamesByStream[s.prevSnapshotStream]
		for _, ss := range s.streamSchema {
			if ss.StreamName == m.Stream {
				for _, col := range ss.Columns {
					colValues = append(colValues, m.Data.AccessProperty(col.Name))
				}
			}
		}

		if pkColName != "" {
			colValues = append(colValues, m.Data.AccessProperty(pkColName))
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

	for bufMIdx, bufMessage := range s.messagesBuffer {
		if bufMIdx == 0 {
			pkColName := s.pkColumnNamesByStream[s.prevSnapshotStream]
			for _, ss := range s.streamSchema {
				if ss.StreamName == bufMessage.Stream {
					for _, col := range ss.Columns {
						colNames = append(colNames, col.Name)
					}
				}
			}

			bufMessage.Data.AccessProperty(s.pkColumnNamesByStream[s.prevSnapshotStream])
			colNames = append(colNames, pkColName)
		}
		var colValues []interface{}
		var pkColValue interface{}
		pkColName := s.pkColumnNamesByStream[s.prevSnapshotStream]
		for _, ss := range s.streamSchema {
			if ss.StreamName == bufMessage.Stream {
				for _, col := range ss.Columns {
					colNames = append(colNames, col.Name)
				}
			}
		}

		bufMessage.Data.AccessProperty(s.pkColumnNamesByStream[s.prevSnapshotStream])
		colNames = append(colNames, pkColName)

		colValues = append(colValues, pkColValue)
		messagesToInsert = append(messagesToInsert, colValues)
	}

	_, err := s.conn.CopyFrom(context.TODO(), pgx.Identifier{s.prevSnapshotStream}, colNames, pgx.CopyFromRows(messagesToInsert))

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
		dbCreateTableStatements = append(dbCreateTableStatements, generateCreateTableStatement(stream.StreamName, stream.Columns))

		insertStatement := generateBatchInsertStatement(stream)
		updateStatement := generateBatchUpdateStatement(stream)
		deleteStatement := generateBatchDeleteStatement(stream)
		rowStatements[stream.StreamName] = map[message.Event]string{
			message.Delete:   deleteStatement,
			message.Update:   updateStatement,
			message.Snapshot: insertStatement,
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
