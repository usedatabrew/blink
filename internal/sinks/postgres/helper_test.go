package postgres

import (
	"fmt"
	"github.com/usedatabrew/blink/internal/schema"
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

func Test_generateBatchInsertStatement(t *testing.T) {
	result := generateBatchInsertStatement(testStreamSchema[0])
	if result != "INSERT INTO flights (id, flights_name) VALUES ($1, $2 );" {
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
