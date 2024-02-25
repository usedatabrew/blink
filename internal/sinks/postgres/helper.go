package postgres

import (
	"fmt"
	"github.com/usedatabrew/blink/internal/helper"
	"github.com/usedatabrew/blink/internal/schema"
	"slices"
	"strings"
)

func generateCreateTableStatement(table string, columns []schema.Column) string {
	spltTable := strings.Split(table, ".")
	table = spltTable[len(spltTable)-1]
	statement := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (\n", table)

	for idx, column := range columns {
		statement += fmt.Sprintf("  %s %s", column.Name, helper.ArrowToPg10(helper.MapPlainTypeToArrow(column.DatabrewType)))
		if column.PK {
			statement += fmt.Sprint(" PRIMARY KEY")
		}
		if !column.Nullable {
			statement += fmt.Sprint(" NOT NULL")
		}
		if idx < len(columns)-1 {
			statement += fmt.Sprint(",\n")
		}
	}

	statement += fmt.Sprint("\n);")
	return statement
}

func generateBatchInsertStatement(table schema.StreamSchema) string {
	columnNames := getColumnNames(table.Columns)
	valuesPlaceholder := getValuesPlaceholder(len(table.Columns))
	return fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES %s;", table.StreamName, columnNames, valuesPlaceholder)
}

func generateBatchUpdateStatement(table schema.StreamSchema) string {
	pkColumns := getPrimaryKeyColumnsForUpdate(table.Columns)
	setClause := getSetClause(table.Columns)

	return fmt.Sprintf("UPDATE \"%s\" SET %s WHERE %s;\n", table.StreamName, setClause, pkColumns)
}

func generateBatchDeleteStatement(table schema.StreamSchema) string {
	pkColumns := getPrimaryKeyColumns(table.Columns)

	return fmt.Sprintf("DELETE FROM \"%s\" WHERE %s;\n", table.StreamName, pkColumns)
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

func getPrimaryKeyColumns(columns []schema.Column) string {
	var pkColumns []string
	for _, column := range columns {
		if column.PK {
			pkColumns = append(pkColumns, fmt.Sprintf("%s = $1", column.Name))
		}
	}
	return strings.Join(pkColumns, " AND ")
}

func getPrimaryKeyColumnsForUpdate(columns []schema.Column) string {
	var pkColumns []string
	for _, column := range columns {
		if column.PK {
			pkColumns = append(pkColumns, fmt.Sprintf("%s = $%d", column.Name, len(columns)))
		}
	}
	return strings.Join(pkColumns, " AND ")
}

func getSetClause(columns []schema.Column) string {
	var setClauses []string
	var bindIndex = 1
	for _, column := range columns {
		// Exclude primary key columns from the set clause
		if !column.PK {
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", column.Name, bindIndex))
			bindIndex += 1
		}
	}
	return strings.Join(setClauses, ", ")
}

func generateStreamNameWithPrefix(stream, prefix string) string {
	var streamSplit = strings.Split(stream, ".")
	if len(streamSplit) == 2 {
		return prefix + streamSplit[1]
	}

	return prefix + stream
}
