package message

import (
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/cloudquery/plugin-sdk/v4/scalar"
	"github.com/google/uuid"
)

type Message struct {
	ID   uint32 `json:"id"`
	Data arrow.Record
	meta map[string]string
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

func (m *Message) SetNewField(name string, value interface{}, fieldType arrow.DataType) {
	newSchemaFields := m.Data.Schema().Fields()
	newFieldType := inferArrowType(value)
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
			if err := s.Set(getValue(m.Data.Column(i), 0)); err != nil {
				panic(err)
			}
		}

		scalar.AppendToBuilder(updatedBuilder.Field(i), s)
	}

	m.Data = updatedBuilder.NewRecord()
}
