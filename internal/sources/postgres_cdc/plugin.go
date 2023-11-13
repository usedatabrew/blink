package postgres_cdc

import (
	"astro/internal/message"
	"astro/internal/schema"
	"astro/internal/sources"
	"context"
	"fmt"
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
		ReplicationSlotName:        fmt.Sprintf("rs_%s", "random_slot_name"),
		TlsVerify:                  "require",
		StreamOldData:              p.config.StreamSnapshot,
		SnapshotMemorySafetyFactor: 0.3,
		BatchSize:                  10000,
		SeparateChanges:            true,
	})
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
		case snapshotMessage := <-p.stream.SnapshotMessageC():
			m := snapshotMessage.Changes[0].Row
			p.messagesStream <- sources.MessageEvent{
				Message: message.New(m),
				Err:     nil,
			}
		case lrMessage := <-p.stream.LrMessageC():
			m := lrMessage.Changes[0].Row
			p.messagesStream <- sources.MessageEvent{
				Message: message.New(m),
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
		tSch.Table = stream.StreamName
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
	fmt.Println(tablesSchema)
	return tablesSchema
}
