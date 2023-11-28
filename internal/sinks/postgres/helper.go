package postgres

import (
	"blink/internal/message"
	"blink/internal/schema"
	"fmt"
	"github.com/apache/arrow/go/v14/arrow"
	"strings"
)

func MapPlainTypeToArrow(fieldType string) arrow.DataType {
	switch fieldType {
	case "Boolean":
		return arrow.FixedWidthTypes.Boolean
	case "Int16":
		return arrow.PrimitiveTypes.Int16
	case "Int32":
		return arrow.PrimitiveTypes.Int32
	case "Int64":
		return arrow.PrimitiveTypes.Int64
	case "Uint64":
		return arrow.PrimitiveTypes.Uint64
	case "Float64":
		return arrow.PrimitiveTypes.Float64
	case "Float32":
		return arrow.PrimitiveTypes.Float32
	case "UUID":
		return arrow.BinaryTypes.String
	case "bytea":
		return arrow.BinaryTypes.Binary
	case "JSON":
		return arrow.BinaryTypes.String
	case "Inet":
		return arrow.BinaryTypes.String
	case "MAC":
		return arrow.BinaryTypes.String
	case "Date32":
		return arrow.FixedWidthTypes.Date32
	default:
		return arrow.BinaryTypes.String
	}
}

func generateCreateTableStatement(table string, columns []schema.Column) string {
	statement := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", table)

	for idx, column := range columns {
		statement += fmt.Sprintf("  %s %s", column.Name, message.ArrowToPg10(MapPlainTypeToArrow(column.DatabrewType)))
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

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;", table.StreamName, columnNames, valuesPlaceholder)
}

func generateBatchUpdateStatement(table schema.StreamSchema) string {
	pkColumns := getPrimaryKeyColumnsForUpdate(table.Columns)
	setClause := getSetClause(table.Columns)

	return fmt.Sprintf("UPDATE %s SET %s WHERE %s;\n", table.StreamName, setClause, pkColumns)
}

func generateBatchDeleteStatement(table schema.StreamSchema) string {
	pkColumns := getPrimaryKeyColumns(table.Columns)

	return fmt.Sprintf("DELETE FROM %s WHERE %s;\n", table.StreamName, pkColumns)
}

func getColumnNames(columns []schema.Column) string {
	var columnNames []string
	var pkColumn string
	for _, column := range columns {
		if column.PK {
			pkColumn = column.Name
		} else {
			columnNames = append(columnNames, column.Name)
		}
	}
	columnNames = append(columnNames, pkColumn)
	return strings.Join(columnNames, ", ")
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
