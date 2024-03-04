package postgres_incr_sync

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/charmbracelet/log"
	"github.com/cloudquery/plugin-sdk/v4/scalar"
	"github.com/jackc/pgx/v5"
	"github.com/usedatabrew/blink/internal/offset_storage"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sources"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/message"
	"strings"
	"time"
)

type SourcePlugin struct {
	ctx    context.Context
	config Config

	syncTicker            *time.Ticker
	appCtx                *stream_context.Context
	streamSchema          []schema.StreamSchema
	streamSchemaMap       map[string]schema.StreamSchema
	streamPks             map[string]string
	logger                *log.Logger
	streamColumnsToSelect map[string][]string
	pgConn                *pgx.Conn
	messagesStream        chan sources.MessageEvent
}

func NewPostgresIncrSourcePlugin(appCtx *stream_context.Context, config Config, s []schema.StreamSchema) sources.DataSource {
	plugin := &SourcePlugin{
		appCtx:                appCtx,
		syncTicker:            time.NewTicker(time.Second * 5),
		config:                config,
		streamSchema:          s,
		logger:                appCtx.Logger.WithPrefix("Source [postgres_incremental_sync]"),
		streamSchemaMap:       map[string]schema.StreamSchema{},
		streamPks:             map[string]string{},
		streamColumnsToSelect: map[string][]string{},
		messagesStream:        make(chan sources.MessageEvent),
	}

	plugin.buildSchemaMap()
	return plugin
}

func (p *SourcePlugin) Connect(ctx context.Context) error {
	sslVerifySettings := ""
	if p.config.SSLRequired {
		sslVerifySettings = "?sslmode=verify-full"
	}

	link := fmt.Sprintf("postgres://%s:%s@%s:%d/%s%s",
		p.config.User,
		p.config.Password,
		p.config.Host,
		p.config.Port,
		p.config.Database,
		sslVerifySettings,
	)

	conn, err := pgx.Connect(ctx, link)
	if err != nil {
		return err
	}

	p.pgConn = conn
	p.ctx = ctx
	return nil
}

func (p *SourcePlugin) Events() chan sources.MessageEvent {
	return p.messagesStream
}

func (p *SourcePlugin) Start() {
	for {
		select {
		case <-p.syncTicker.C:
			for _, stream := range p.streamSchema {
				var offset int64 = 0
				streamStoredOffset, _ := p.appCtx.OffsetStorage().GetOffsetByPipelineStream(offset_storage.BuildKey(p.appCtx.PipelineId(), stream.StreamName))
				if !p.config.StreamSnapshot {
					if streamStoredOffset == 0 {
						offset = p.selectLastRecordId(stream.StreamName)
						p.logger.Info("Snapshot streaming is disabled. Starting sync form the offset", "offset", offset)
					} else {
						offset = streamStoredOffset
					}
				} else {
					if streamStoredOffset > 0 {
						offset = streamStoredOffset
						p.logger.Info("Stream snapshot enabled, continue from stored offset", "offset", 0)
					} else {
						p.logger.Info("Stream snapshot enabled, not stored offset not found. Starting from 0", "offset", 0)
					}
				}

				if schem, ok := p.streamSchemaMap[stream.StreamName]; ok {
					arrowSchema := schem.AsArrow()
					builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
					pkForStream := p.streamPks[stream.StreamName]
					selectColumns := strings.Join(p.streamColumnsToSelect[stream.StreamName], ", ")
					q := fmt.Sprintf("SELECT %s FROM %s WHERE %s > %d ORDER BY %s ASC LIMIT 10000", selectColumns, stream.StreamName, pkForStream, offset, pkForStream)
					rows, err := p.pgConn.Query(p.ctx, q)
					if err != nil {
						p.appCtx.Logger.Fatalf("Failed to query data for offset %s", err.Error())
					}

					var rowsFetched = 0
					for rows.Next() {
						rowsFetched += 1
						values, err := rows.Values()
						if err != nil {
							panic(err)
						}

						for i, v := range values {
							s := scalar.NewScalar(arrowSchema.Field(i).Type)
							if err := s.Set(v); err != nil {
								panic(err)
							}

							scalar.AppendToBuilder(builder.Field(i), s)
						}

						data, _ := builder.NewRecord().MarshalJSON()
						splitStream := strings.Split(stream.StreamName, ".")
						m := message.NewMessage(message.Insert, splitStream[len(splitStream)-1], data)
						p.messagesStream <- sources.MessageEvent{
							Message: m,
							Err:     err,
						}
					}

					if rowsFetched == 0 {
						p.logger.Info("No rows were fetched, waiting for the data to appear...")
						p.logger.Info("Updating offset", "offset", offset)
						err = p.appCtx.OffsetStorage().SetOffsetForPipeline(offset_storage.BuildKey(p.appCtx.PipelineId(), stream.StreamName), offset)
						if err != nil {
							p.logger.Fatalf("Failed to store the offset %s", err.Error())
						}
						continue
					}

					if rowsFetched == 0 && streamStoredOffset == 0 {
						p.logger.Info("No rows fetched after the initial connection. Waiting for the first new rows to appear")
						p.logger.Info("Storing default offset", "offset", offset)
						err = p.appCtx.OffsetStorage().SetOffsetForPipeline(offset_storage.BuildKey(p.appCtx.PipelineId(), stream.StreamName), offset)
						if err != nil {
							p.logger.Fatalf("Failed to store the offset %s", err.Error())
						}
					} else {
						newOffset := streamStoredOffset + int64(rowsFetched)
						p.logger.Infof("Fetched %d storing new offset %d", rowsFetched, newOffset)
						err = p.appCtx.OffsetStorage().SetOffsetForPipeline(offset_storage.BuildKey(p.appCtx.PipelineId(), stream.StreamName), newOffset)
						if err != nil {
							p.logger.Fatalf("Failed to store the offset %s", err.Error())
						}
					}
				}
			}
		}
	}

}

func (p *SourcePlugin) Stop() {
	p.syncTicker.Stop()
}

func (p *SourcePlugin) selectLastRecordId(streamName string) int64 {
	pkForStream := p.streamPks[streamName]
	res := p.pgConn.QueryRow(context.Background(), fmt.Sprintf("SELECT MAX(%s) as last_row FROM %s", pkForStream, streamName))
	var result int64
	err := res.Scan(&result)
	if err != nil {
		panic(err)
	}

	return result
}

func (p *SourcePlugin) buildSchemaMap() {
	for _, stream := range p.streamSchema {
		var columnsToSelect []string
		p.streamSchemaMap[stream.StreamName] = stream
		for _, c := range stream.Columns {
			if c.PK {
				p.streamPks[stream.StreamName] = c.Name
			}
			columnsToSelect = append(columnsToSelect, c.Name)
		}
		p.streamColumnsToSelect[stream.StreamName] = columnsToSelect
	}
}
