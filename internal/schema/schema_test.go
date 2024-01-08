package schema

import (
	"bytes"
	"fmt"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/goccy/go-json"
	"github.com/zeebo/assert"
	"gopkg.in/yaml.v3"
	"os"
	"testing"
)

var messagePacket = `{ 
	"coin": "BTC", "price": 123.11,
	"data_points": [11,22,333], 
	"market": {
		"market_name": "binance", "market_size": 123
	},
	"markets": [
			{ "market_name": "binance", "market_size": 123, "owner": { "name": "Vladyslav", "ids": [123,123,211] } }
		]
	}`

func Test_StreamSchema_parsing(t *testing.T) {
	schemaBytes, _ := os.ReadFile("./mocks/schema.yaml")
	var decodedYamlSchema StreamSchema
	err := yaml.NewDecoder(bytes.NewReader(schemaBytes)).Decode(&decodedYamlSchema)
	assert.NoError(t, err)
	arrowSchema := decodedYamlSchema.AsArrow()
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	//var res interface{}
	assert.NoError(t, json.Unmarshal([]byte(messagePacket), &b))
	mesB, err := b.NewRecord().MarshalJSON()
	assert.NoError(t, err)
	fmt.Println(string(mesB))
}
