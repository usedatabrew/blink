package postgres

import (
	"fmt"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/message"
	"testing"
)

var testStreamSchema = []schema.StreamSchema{
	{
		StreamName: "flights",
		Columns: []schema.Column{
			{
				Name:         "id",
				DatabrewType: "Int",
				PK:           true,
				Nullable:     false,
			},
			{
				Name:         "flights_name",
				DatabrewType: "String",
				Nullable:     false,
				PK:           false,
			},
		},
	},
}

var testMessage = message.NewMessage(message.Snapshot, "flights", []byte(`{"message": "code_11", "id": 1234}`))

func Test_generateBatchInsertStatement(t *testing.T) {
	result := generateBatchInsertStatement(testStreamSchema[0])
	if result != "INSERT INTO flights (flights_name, id) VALUES ($1, $2 );" {
		t.Fatal("Generated Insert Query is not correct")
	}
}

func Test_generateBatchUpdateStatement(t *testing.T) {
	result := generateBatchUpdateStatement(testStreamSchema[0])
	if result != "UPDATE flights SET flights_name = $1 WHERE id = $2;\n" {
		t.Fatal("Generated Update Query is not correct")
	}
}

func Test_generateBatchDeleteStatement(t *testing.T) {
	result := generateBatchDeleteStatement(testStreamSchema[0])
	fmt.Println(result)
	if result != "DELETE FROM flights WHERE id = $1;\n" {
		t.Fatal("Generated Update Query is not correct")
	}
}

func Test_getColumnNames(t *testing.T) {
	generatedColumnNames := getColumnNames(testStreamSchema[0].Columns)
	fmt.Println(generatedColumnNames)
}

func TestSinkPlugin_Write(t *testing.T) {

}

func Test_generateCreateTableStatement(t *testing.T) {
	generatedStream := generateStreamNameWithPrefix("public.zenko_comments", "databrew_")
	fmt.Println(generatedStream)
}
