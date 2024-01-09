package message

import (
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/cloudquery/plugin-sdk/v4/scalar"
	"github.com/google/uuid"
	"github.com/usedatabrew/blink/internal/helper"
	"slices"
	"sync"
)

type Message struct {
	ID   uint32 `json:"id"`
	Data arrow.Record
	meta map[string]string
	Err  error
	lock sync.Mutex
}

func New(data arrow.Record) Message {
	return Message{
		Data: data,
		ID:   uuid.New().ID(),
		meta: make(map[string]string),
	}
}

// SetStream is used to specify table or topic
// it's required to map message to database table, collection or kafka topic
// each message must have a stream definition
func (m *Message) SetStream(name string) {
	m.meta["stream"] = name
}

// SetEvent is used to specify event that happened to the message
// Be the default messages are produced with insert event
// this event is used by the processors and sinks to determine the operation
// that should be executed for the message. Like delete it from the sink database (if supported)
func (m *Message) SetEvent(name string) {
	m.meta["event"] = name
}

// Set method simply adds a key for the message metadata map
func (m *Message) Set(name, value string) {
	m.meta[name] = value
}

func (m *Message) GetStream() string {
	v, _ := m.meta["stream"]

	return v
}

func (m *Message) GetEvent() string {
	v, _ := m.meta["event"]

	return v
}

func (m *Message) GetValue(colName string) interface{} {
	for idx, col := range m.Data.Columns() {
		if m.Data.Schema().Field(idx).Name == colName {
			return helper.GetValue(col, 0)
		}
	}
	return nil
}

func (m *Message) SetNewField(name string, value interface{}, fieldType arrow.DataType) {
	m.lock.Lock()
	defer m.lock.Unlock()
	newSchemaFields := m.Data.Schema().Fields()
	newFieldType := helper.InferArrowType(value)
	updatedSchema := arrow.NewSchema(append(newSchemaFields, arrow.Field{Name: name, Type: newFieldType}), nil)
	updatedBuilder := array.NewRecordBuilder(memory.DefaultAllocator, updatedSchema)
	for i, field := range updatedSchema.Fields() {
		var s scalar.Scalar
		if field.Name == name {
			s = scalar.NewScalar(newFieldType)
			if err := s.Set(value); err != nil {
				panic(err)
			}
		} else {
			s = scalar.NewScalar(field.Type)
			if err := s.Set(helper.GetValue(m.Data.Column(i), 0)); err != nil {
				panic(err)
			}
		}

		scalar.AppendToBuilder(updatedBuilder.Field(i), s)
	}

	m.Data = updatedBuilder.NewRecord()
}

// RemoveFields removes the specified fields from the Message data schema.
// It locks the Message for thread safety and releases the lock at the end.
// It creates a new set of fields excluding the ones specified in the 'fields' parameter.
// It then creates a new schema with the updated fields and a new record builder using the updated schema.
// It copies the values of the remaining fields from the original record to the new builder.
// Finally, it sets the Message data to the new record.
//
// Parameters:
//   - fields: A string array containing the names of the fields to be removed from the schema.
//     If a field is not found, it is ignored.
func (m *Message) RemoveFields(fields []string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	var newFields []arrow.Field
	for _, field := range m.Data.Schema().Fields() {
		if !slices.Contains(fields, field.Name) {
			newFields = append(newFields, field)
		}

	}

	updatedSchema := arrow.NewSchema(newFields, nil)
	updatedBuilder := array.NewRecordBuilder(memory.DefaultAllocator, updatedSchema)

	for i, field := range newFields {
		s := scalar.NewScalar(field.Type)

		if err := s.Set(m.GetValue(field.Name)); err != nil {
			panic(err)
		}

		scalar.AppendToBuilder(updatedBuilder.Field(i), s)
	}

	m.Data = updatedBuilder.NewRecord()
}
