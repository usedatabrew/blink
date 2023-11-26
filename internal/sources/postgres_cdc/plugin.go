package postgres_cdc

import (
	"blink/internal/message"
	"blink/internal/schema"
	"blink/internal/sources"
	"context"
	"fmt"
	"github.com/charmbracelet/log"
	"github.com/usedatabrew/pglogicalstream"
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
	pgStream, err := pglogicalstream.NewPgStream(pglogicalstream.Config{
		DbHost:                     p.config.Host,
		DbPassword:                 p.config.Password,
		DbUser:                     p.config.User,
		DbPort:                     p.config.Port,
		DbName:                     p.config.Database,
		DbSchema:                   p.config.Schema,
		DbTablesSchema:             p.buildPluginsSchema(),
		ReplicationSlotName:        fmt.Sprintf("rs_%s", p.config.SlotName),
		TlsVerify:                  "require",
		StreamOldData:              p.config.StreamSnapshot,
		SnapshotMemorySafetyFactor: 0.3,
		BatchSize:                  13500,
		SeparateChanges:            true,
	}, log.WithPrefix("PostgreSQL-CDC"))
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
			builtMessage := message.New(m)
			builtMessage.SetEvent(snapshotMessage.Changes[0].Kind)
			builtMessage.SetStream(snapshotMessage.Changes[0].Table)
			p.messagesStream <- sources.MessageEvent{
				Message: builtMessage,
				Err:     nil,
			}
		case lrMessage := <-p.stream.LrMessageC():
			m := lrMessage.Changes[0].Row
			builtMessage := message.New(m)
			builtMessage.SetEvent(lrMessage.Changes[0].Kind)
			builtMessage.SetStream(lrMessage.Changes[0].Table)
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
		tSch.Table = fmt.Sprintf("%s.%s", p.config.Schema, stream.StreamName)
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
