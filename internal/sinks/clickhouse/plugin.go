package clickhouse

import (
	"context"
	"fmt"
	"net"
	"time"

	clickhouseClient "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/charmbracelet/log"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sinks"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/message"
)

type SinkPlugin struct {
	ctx           *stream_context.Context
	config        Config
	inputSchema   map[string]schema.StreamSchema
	logger        *log.Logger
	connection    driver.Conn
	rowStatements map[string]map[message.Event]string
}

func NewClickHouseSinkPlugin(config Config, ctx *stream_context.Context) sinks.DataSink {
	return &SinkPlugin{
		ctx:    ctx,
		config: config,
		logger: ctx.Logger.WithPrefix("[sink]: clickhouse"),
	}
}

func (p *SinkPlugin) Connect(ctx context.Context) error {
	host := fmt.Sprintf("%s:%d", p.config.Host, p.config.Port)

	conn, err := clickhouseClient.Open(&clickhouseClient.Options{
		Addr: []string{host},
		Auth: clickhouseClient.Auth{
			Database: p.config.Database,
			Username: p.config.User,
			Password: p.config.Password,
		},
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
		Settings: clickhouseClient.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouseClient.Compression{
			Method: clickhouseClient.CompressionLZ4,
		},
		DialTimeout:          time.Second * 30,
		MaxOpenConns:         5,
		MaxIdleConns:         5,
		ConnOpenStrategy:     clickhouseClient.ConnOpenInOrder,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
	})

	if err != nil {
		return err
	}

	conn.Ping(p.ctx.GetContext())

	p.connection = conn

	return nil
}

func (p *SinkPlugin) Write(m *message.Message) error {
	event := m.GetEvent()

	if event != message.Insert {
		return nil
	}

	p.logger.Info("Applying operation", "op", m.GetEvent(), "stream", m.GetStream())

	var colValues []interface{}
	for _, col := range getColumnNamesSorted(p.inputSchema[m.GetStream()].Columns) {
		colValues = append(colValues, m.Data.AccessProperty(col))
	}

	statement := p.rowStatements[m.GetStream()][m.GetEvent()]

	err := p.connection.AsyncInsert(p.ctx.GetContext(), statement, false, colValues...)

	if err != nil {
		return nil
	}

	return nil
}

func (p *SinkPlugin) GetType() sinks.SinkDriver {
	return sinks.ClickHouse
}

func (p *SinkPlugin) SetExpectedSchema(sCh []schema.StreamSchema) {
	iSchema := make(map[string]schema.StreamSchema)

	for _, stream := range sCh {
		iSchema[stream.StreamName] = stream
	}

	p.inputSchema = iSchema

	p.createInitStatements()
}

func (p *SinkPlugin) Stop() {
	p.connection.Close()
}

func (p *SinkPlugin) createInitStatements() {
	dbCreateTableStatements := make(map[string]string)
	var rowStatements = make(map[string]map[message.Event]string)

	for _, stream := range p.inputSchema {
		dbCreateTableStatements[stream.StreamName] = generateCreateTableStatement(stream.StreamName, stream.Columns)

		insertStatement := generateInsertStatement(stream)

		rowStatements[stream.StreamName] = map[message.Event]string{
			message.Insert: insertStatement,
		}
	}

	p.rowStatements = rowStatements

	p.logger.Info("Generated init statements to create table for the sink database", "statements", dbCreateTableStatements)

	for streamName, dbCreateTableStatement := range dbCreateTableStatements {
		err := p.connection.Exec(p.ctx.GetContext(), dbCreateTableStatement)

		if err != nil {
			p.logger.Fatal("Failed to create table for stream", "stream", streamName, "error", err)
		}
	}
}
