package clickhouse

import (
	"fmt"
	"slices"
	"strings"

	"github.com/usedatabrew/blink/internal/helper"
	"github.com/usedatabrew/blink/internal/schema"
)

func generateCreateTableStatement(table string, columns []schema.Column) string {
	statement := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (\n", table)
	var primaryKeys []string

	for idx, column := range columns {
		statement += fmt.Sprintf("  %s %s", column.Name, helper.ArrowToClickHouse(helper.MapPlainTypeToArrow(column.DatabrewType)))
		if column.PK {
			primaryKeys = append(primaryKeys, column.Name)
		}
		if !column.Nullable {
			statement += " NOT NULL"
		}
		if idx < len(columns)-1 {
			statement += ",\n"
		}
	}

	statement += "\n)"

	statement += " ENGINE = Memory()\n"
	statement += fmt.Sprintf("PRIMARY KEY(%v)", primaryKeys)

	return statement
}

func generateInsertStatement(table schema.StreamSchema) string {
	columnNames := getColumnNames(table.Columns)
	valuesPlaceholder := getValuesPlaceholder(len(table.Columns))

	return fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES %s;", table.StreamName, columnNames, valuesPlaceholder)
}

func getColumnNames(columns []schema.Column) string {
	return strings.Join(getColumnNamesSorted(columns), ", ")
}

func getColumnNamesSorted(columns []schema.Column) []string {
	var columnNames []string
	for _, column := range columns {
		columnNames = append(columnNames, column.Name)
	}

	slices.Sort(columnNames)

	return columnNames
}

func getValuesPlaceholder(numColumns int) string {
	var valuePlaceholders []string
	for i := 0; i < numColumns; i++ {
		valuePlaceholders = append(valuePlaceholders, fmt.Sprintf("$%d", i+1))
	}
	return fmt.Sprintf("(%s )", strings.Join(valuePlaceholders, ", "))
}
