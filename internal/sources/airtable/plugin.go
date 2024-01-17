package airtable

import (
	"context"
	"errors"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/charmbracelet/log"
	"github.com/goccy/go-json"
	"github.com/mehanizm/airtable"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sources"
	"github.com/usedatabrew/message"
	"strings"
	"time"
)

type SourcePlugin struct {
	messageEvents      chan sources.MessageEvent
	config             Config
	client             *airtable.Client
	schema             []schema.StreamSchema
	schemaMapper       map[string]schema.StreamSchema
	streamPks          map[string]string
	streamFields       map[string][]string
	streamOffsets      map[string]string
	lastRecordStreamId map[string]string
	logger             *log.Logger
}

func NewAirTableSourcePlugin(config Config, s []schema.StreamSchema) sources.DataSource {
	plugin := &SourcePlugin{
		config:             config,
		schema:             s,
		messageEvents:      make(chan sources.MessageEvent),
		schemaMapper:       map[string]schema.StreamSchema{},
		streamFields:       map[string][]string{},
		streamOffsets:      map[string]string{},
		lastRecordStreamId: map[string]string{},
		streamPks:          map[string]string{},
		logger:             log.WithPrefix("[source]: AirTable"),
	}

	plugin.extractFields()

	return plugin
}

func (s *SourcePlugin) Connect(ctx context.Context) error {
	s.client = airtable.NewClient(s.config.ApiKey)
	if err := s.client.SetBaseURL("https://api.airtable.com/v0"); err != nil {
		return err
	}
	_, err := s.client.GetBases().Do()
	if err != nil {
		return err
	}

	return err
}

func (s *SourcePlugin) Start() {
	for {
		for stream, v := range s.streamFields {
			streamSchemaByName, ok := s.schemaMapper[stream]
			if !ok {
				s.messageEvents <- sources.MessageEvent{
					Message: nil,
					Err:     errors.New("failed to extract stream schema"),
				}
				continue
			}

			builder := array.NewRecordBuilder(memory.DefaultAllocator, streamSchemaByName.AsArrow())
			splitStream := strings.Split(stream, ".")
			getRowsRequest := s.client.GetTable(splitStream[0], splitStream[1]).GetRecords()

			getRowsRequest.ReturnFields(v...)

			streamOffset, offsetExists := s.streamOffsets[stream]
			queryingWithOffset := offsetExists && streamOffset != ""
			if queryingWithOffset {
				getRowsRequest.WithOffset(streamOffset)
			} else {
				s.logger.Info("Initial load. Querying without offset")
			}

			if s.streamPks[stream] != "" {
				getRowsRequest.WithSort(struct {
					FieldName string
					Direction string
				}{FieldName: s.streamPks[stream], Direction: "asc"})
			}

			getRowsRequest.PageSize(2)
			result, err := getRowsRequest.Do()
			if err != nil {
				s.messageEvents <- sources.MessageEvent{
					Message: nil,
					Err:     err,
				}
				continue
			}

			if queryingWithOffset && streamOffset == result.Offset {
				s.logger.Info("All the records fetched. Skipping the iteration", "stream", splitStream[1])
				continue
			} else {
				s.logger.Info("Querying records with offset", "offset", streamOffset)
			}

			lastRecordId := result.Records[len(result.Records)-1].ID
			lastRecordIdStored, lastRecordExist := s.lastRecordStreamId[stream]
			if lastRecordExist && lastRecordIdStored == lastRecordId {
				continue
			}

			for _, result := range result.Records {
				var data []byte
				if data, err = json.Marshal(result.Fields); err != nil {
					s.messageEvents <- sources.MessageEvent{
						Message: nil,
						Err:     err,
					}
					continue
				}

				if err = json.Unmarshal(data, &builder); err != nil {
					s.messageEvents <- sources.MessageEvent{
						Message: nil,
						Err:     err,
					}
					continue
				}

				data, _ = builder.NewRecord().MarshalJSON()
				m := message.NewMessage(message.Insert, splitStream[1], data)
				s.messageEvents <- sources.MessageEvent{
					Message: m,
					Err:     nil,
				}
			}
			if result.Offset != "" {
				s.streamOffsets[stream] = result.Offset
				s.logger.Info("Storing offset for stream", "stream", splitStream[1], "offset", result.Offset)
			} else {
				s.logger.Info("All records were fetched for stream", "stream", splitStream[1])
			}
			s.lastRecordStreamId[stream] = lastRecordId
		}
		<-time.After(time.Second * 25)
	}
}

func (s *SourcePlugin) Events() chan sources.MessageEvent {
	return s.messageEvents
}

func (s *SourcePlugin) Stop() {}

func (s *SourcePlugin) extractFields() {
	for _, stream := range s.schema {
		var fields []string
		for _, col := range stream.Columns {
			fields = append(fields, col.Name)
			if col.PK {
				s.streamPks[stream.StreamName] = col.Name
			}
		}

		s.schemaMapper[stream.StreamName] = stream
		s.streamFields[stream.StreamName] = fields
	}
}
