package mysql_cdc

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/cloudquery/plugin-sdk/v4/scalar"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/usedatabrew/blink/internal/helper"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sources"
	"github.com/usedatabrew/message"
)

type DataTableSchema struct {
	TableName string
	Schema    *arrow.Schema
}

type ProcessEventParams struct {
	initValue, incrementValue int
}

type SourcePlugin struct {
	config         Config
	inputSchema    map[string]schema.StreamSchema
	outputSchema   map[string]DataTableSchema
	messagesStream chan sources.MessageEvent
	canal          *canal.Canal
	canal.DummyEventHandler
}

func NewMysqlSourcePlugin(config Config, sCh []schema.StreamSchema) sources.DataSource {
	iSchema := make(map[string]schema.StreamSchema)

	for _, stream := range sCh {
		iSchema[stream.StreamName] = stream
	}

	instance := &SourcePlugin{
		config:         config,
		inputSchema:    iSchema,
		messagesStream: make(chan sources.MessageEvent),
	}

	instance.buildOutputSchema()

	return instance
}

func (p *SourcePlugin) Connect(ctx context.Context) error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", p.config.Host, p.config.Port)
	cfg.User = p.config.User
	cfg.Password = p.config.Password
	cfg.Flavor = p.config.Flavor

	cfg.Dump.TableDB = p.config.Database

	var tables []string

	for _, table := range p.inputSchema {
		tables = append(tables, table.StreamName)
	}

	cfg.Dump.Tables = tables

	c, err := canal.NewCanal(cfg)

	if err != nil {
		return err
	}

	p.canal = c

	return nil
}

func (p *SourcePlugin) Start() {
	p.canal.SetEventHandler(p)

	if p.config.StreamSnapshot {
		p.canal.Run()
	} else {
		coords, _ := p.canal.GetMasterPos()

		p.canal.RunFrom(coords)
	}
}

func (p *SourcePlugin) Stop() {
	p.canal.Close()
}

func (p *SourcePlugin) Events() chan sources.MessageEvent {
	return p.messagesStream
}

func (p *SourcePlugin) OnRow(e *canal.RowsEvent) error {
	if p.config.Database != e.Table.Schema {
		return nil
	}

	if _, ok := p.inputSchema[e.Table.Name]; !ok {
		return nil
	}

	switch e.Action {
	case canal.InsertAction:
		return p.processEvent(e, ProcessEventParams{initValue: 0, incrementValue: 1})
	case canal.DeleteAction:
		return p.processEvent(e, ProcessEventParams{initValue: 0, incrementValue: 1})
	case canal.UpdateAction:
		return p.processEvent(e, ProcessEventParams{initValue: 1, incrementValue: 2})
	default:
		return errors.New("invalid rows action")
	}
}

func (p *SourcePlugin) processEvent(e *canal.RowsEvent, params ProcessEventParams) error {
	inputSchema := p.inputSchema[e.Table.Name]
	outputSchema := p.outputSchema[e.Table.Name]

	builder := array.NewRecordBuilder(memory.DefaultAllocator, outputSchema.Schema)

	for i := params.initValue; i < len(e.Rows); i += params.incrementValue {
		for i, v := range e.Rows[i] {
			outputIndex := -1

			for inputSchemaIndex, inputSchemaColumn := range inputSchema.Columns {
				if e.Table.Columns[i].Name == inputSchemaColumn.Name {
					outputIndex = inputSchemaIndex
				}
			}

			if outputIndex == -1 {
				continue
			}

			s := scalar.NewScalar(outputSchema.Schema.Field(outputIndex).Type)

			if err := s.Set(convertData(e.Table.Columns[i], v)); err != nil {
				panic(err)
			}

			scalar.AppendToBuilder(builder.Field(outputIndex), s)
		}
	}

	bytes, _ := builder.NewRecord().MarshalJSON()
	m := message.NewMessage(message.Event(e.Action), e.Table.Name, bytes)

	p.messagesStream <- sources.MessageEvent{
		Message: m,
		Err:     nil,
	}

	return nil
}

func (p *SourcePlugin) buildOutputSchema() {
	outputSchema := make(map[string]DataTableSchema)

	for _, stream := range p.inputSchema {
		tSch := DataTableSchema{
			TableName: stream.StreamName,
		}

		var arrowSchemaFields []arrow.Field

		for _, schemaCol := range stream.Columns {
			arrowSchemaFields = append(arrowSchemaFields, arrow.Field{
				Name:     schemaCol.Name,
				Type:     helper.MapPlainTypeToArrow(schemaCol.DatabrewType),
				Nullable: schemaCol.Nullable,
				Metadata: arrow.Metadata{},
			})
		}

		tSch.Schema = arrow.NewSchema(arrowSchemaFields, nil)
		outputSchema[stream.StreamName] = tSch
	}

	p.outputSchema = outputSchema
}
