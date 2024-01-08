package sqlproc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	_ "github.com/apache/arrow/go/v14/arrow"
	"github.com/barkimedes/go-deepcopy"
	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/charmbracelet/log"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/message"
	"math"
	"slices"
	"strconv"
	"strings"
)

type Plugin struct {
	config                   Config
	resultSchema             []schema.StreamSchema
	ctx                      *stream_context.Context
	logger                   *log.Logger
	columnsToDropFromSchema  []string
	columnNameToIndexBinding map[string]int
	affectedStream           string
	whereExist               bool
	whereLeft                string
	whereOp                  string
	whereRight               interface{}
}

func NewSqlTransformPlugin(appctx *stream_context.Context, config Config) (*Plugin, error) {
	return &Plugin{
		config:                   config,
		ctx:                      appctx,
		logger:                   appctx.Logger.WithPrefix("processor [sql]"),
		columnNameToIndexBinding: make(map[string]int),
		columnsToDropFromSchema:  []string{},
	}, nil
}

func (p *Plugin) Process(context context.Context, msg *message.Message) (*message.Message, error) {
	if msg.GetStream() != p.affectedStream {
		return msg, nil
	}

	if len(p.columnsToDropFromSchema) > 0 {
		for _, prop := range p.columnsToDropFromSchema {
			msg.Data.DropProperty(prop)
		}
	}

	if p.whereExist {
		columnValue := msg.Data.AccessProperty(p.whereLeft)
		if !compareValues(columnValue, p.whereRight, p.whereOp) {
			return nil, nil
		}
	}

	return msg, nil
}

// EvolveSchema will add a string column to the schema in order to match the result to SQL statement
func (p *Plugin) EvolveSchema(streamSchema *schema.StreamSchemaObj) error {
	stmt, err := sqlparser.Parse(p.config.Query)
	if err != nil {
		return err
	}

	var streamName string
	// processor can be applied only for a single stream
	// no joins are supported at the moment
	if len(stmt.(*sqlparser.Select).From) != 1 {
		return errors.New("exactly one select statement expected for the stream")
	} else {
		streamName = stmt.(*sqlparser.Select).From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()
		p.affectedStream = streamName
	}

	var streamToProcess *schema.StreamSchema
	for _, stream := range streamSchema.GetLatestSchema() {
		if stream.StreamName == streamName {
			streamToProcess = &stream
		}
	}

	if streamToProcess == nil {
		return errors.New("select from undefined stream")
	}

	// check if user wants to select all the columns from the stream
	// in this case we don't have to remove columns from the schema
	if _, ok := stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.StarExpr); !ok {
		// select expression is not * so we have to check the columns user wants to see in the result
		// contains slice of columns that user wats to have selected
		var columns []string
		for _, selectCol := range stmt.(*sqlparser.Select).SelectExprs {
			columns = append(columns, selectCol.(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName).Name.String())
		}

		// now we have to check is all the columns are available in the schema
		for _, col := range streamToProcess.Columns {
			idx := slices.Index(columns, col.Name)
			if idx == -1 {
				// simple string deep copying doesn't require error handing :(
				colNameCopied, _ := deepcopy.Anything(col.Name)
				p.columnsToDropFromSchema = append(p.columnsToDropFromSchema, colNameCopied.(string))
			} else {
				columns = append(columns[:idx], columns[idx+1:]...)
			}
		}

		// if we have some columns left in the slice
		// it means that user wants to select more columns than we have
		// in the current stream schema version
		if len(columns) > 0 {
			return errors.New(fmt.Sprintf("undefined columns selection %s", strings.Join(columns, ", ")))
		}
	}

	// checking for where condition
	if stmt.(*sqlparser.Select).Where != nil {
		p.whereExist = true
		// checking for filtering conditions
		p.whereOp = stmt.(*sqlparser.Select).Where.Expr.(*sqlparser.ComparisonExpr).Operator
		whereColumn := stmt.(*sqlparser.Select).Where.Expr.(*sqlparser.ComparisonExpr).Left.(*sqlparser.ColName).Name.String()

		// check if column we want to apply where to exist
		whereColumnExist := false
		for _, col := range streamToProcess.Columns {
			if col.Name == whereColumn {
				whereColumnExist = true
				p.whereLeft = col.Name
			}
		}

		if !whereColumnExist {
			return errors.New(fmt.Sprintf("Column %s doesnt exist in current stream", whereColumn))
		}

		rightValType := stmt.(*sqlparser.Select).Where.Expr.(*sqlparser.ComparisonExpr).Right.(*sqlparser.SQLVal).Type
		rightVal := stmt.(*sqlparser.Select).Where.Expr.(*sqlparser.ComparisonExpr).Right.(*sqlparser.SQLVal).Val
		switch rightValType {
		case sqlparser.StrVal:
			p.whereRight = string(rightVal)
		case sqlparser.FloatVal:
			p.whereRight = Float64FromBytes(rightVal)
		case sqlparser.IntVal:
			p.whereRight = Int64FromBytes(rightVal)
		default:
			panic("unhandled default case")
		}
	}

	if len(p.columnsToDropFromSchema) > 0 {
		streamSchema.RemoveFields(streamToProcess.StreamName, p.columnsToDropFromSchema)
	}

	return nil
}

func Float64FromBytes(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	float := math.Float64frombits(bits)
	return float
}

func Int64FromBytes(bytes []byte) int64 {
	i, err := strconv.ParseInt(string(bytes), 10, 64)
	if err != nil {
		panic(err)
	}
	return i
}
