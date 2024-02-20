package postgres_cdc

import (
	"context"
	"fmt"
	"github.com/charmbracelet/log"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sources"
	"github.com/usedatabrew/message"
	"github.com/usedatabrew/pglogicalstream"
	"strings"
)

type SourcePlugin struct {
	ctx            context.Context
	config         Config
	streamSchema   []schema.StreamSchema
	stream         *pglogicalstream.Stream
	messagesStream chan sources.MessageEvent
}

func NewPostgresSourcePlugin(config Config, schema []schema.StreamSchema) sources.DataSource {
	return &SourcePlugin{
		config:         config,
		streamSchema:   schema,
		messagesStream: make(chan sources.MessageEvent),
	}
}

func (p *SourcePlugin) Connect(ctx context.Context) error {
	tlsVerifyMode := pglogicalstream.TlsNoVerify
	if p.config.SSLRequired {
		tlsVerifyMode = pglogicalstream.TlsRequireVerify
	}

	pgStream, err := pglogicalstream.NewPgStream(pglogicalstream.Config{
		DbHost:                     p.config.Host,
		DbPassword:                 p.config.Password,
		DbUser:                     p.config.User,
		DbPort:                     p.config.Port,
		DbName:                     p.config.Database,
		DbSchema:                   p.config.Schema,
		DbTablesSchema:             p.buildPluginsSchema(),
		ReplicationSlotName:        fmt.Sprintf("rs_%s", p.config.SlotName),
		TlsVerify:                  tlsVerifyMode,
		StreamOldData:              p.config.StreamSnapshot,
		SnapshotMemorySafetyFactor: 0.3,
		BatchSize:                  13500,
		SeparateChanges:            true,
	}, log.WithPrefix("[source]: PostgreSQL-CDC"))
	if err != nil {
		return err
	}

	p.stream = pgStream
	p.ctx = ctx
	return nil
}

func (p *SourcePlugin) Events() chan sources.MessageEvent {
	return p.messagesStream
}

func (p *SourcePlugin) Start() {
	for {
		select {
		// snapshot messages channel always opens first
		// so, here we can be positive about message ordering
		// logical replication messages will be streamed after the snapshot is processed
		case snapshotMessage := <-p.stream.SnapshotMessageC():
			m := snapshotMessage.Changes[0].Row
			mbytes, _ := m.MarshalJSON()
			builtMessage := message.NewMessage(message.Snapshot, snapshotMessage.Changes[0].Table, mbytes)
			p.messagesStream <- sources.MessageEvent{
				Message: builtMessage,
				Err:     nil,
			}
		case lrMessage := <-p.stream.LrMessageC():
			m := lrMessage.Changes[0].Row
			mbytes, _ := m.MarshalJSON()
			builtMessage := message.NewMessage(message.Event(lrMessage.Changes[0].Kind), lrMessage.Changes[0].Table, mbytes)
			p.messagesStream <- sources.MessageEvent{
				Message: builtMessage,
				Err:     nil,
			}
			p.stream.AckLSN(lrMessage.Lsn)
		}
	}

}

func (p *SourcePlugin) Stop() {
	err := p.stream.Stop()
	if err != nil {
		fmt.Println("Failed to close producer", err)
	}
}

func (p *SourcePlugin) buildPluginsSchema() []pglogicalstream.DbTablesSchema {
	var tablesSchema []pglogicalstream.DbTablesSchema
	for _, stream := range p.streamSchema {
		tSch := pglogicalstream.DbTablesSchema{}
		if len(strings.Split(stream.StreamName, ".")) == 2 {
			// schema name included in stream.
			tSch.Table = stream.StreamName
		} else {
			tSch.Table = fmt.Sprintf("%s.%s", p.config.Schema, stream.StreamName)
		}
		for _, schemaCol := range stream.Columns {
			tSch.Columns = append(tSch.Columns, pglogicalstream.DbSchemaColumn{
				Name:                schemaCol.Name,
				DatabrewType:        schemaCol.DatabrewType,
				NativeConnectorType: schemaCol.NativeConnectorType,
				Pk:                  schemaCol.PK,
				Nullable:            schemaCol.Nullable,
			})
		}

		tablesSchema = append(tablesSchema, tSch)
	}

	return tablesSchema
}
