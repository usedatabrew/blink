package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/usedatabrew/blink/internal/schema"
	"github.com/usedatabrew/blink/internal/sources"
	"github.com/usedatabrew/message"
)

type SourcePlugin struct {
	ctx           context.Context
	config        Config
	client        *kgo.Client
	stream        string
	inputSchema   []schema.StreamSchema
	outputSchema  map[string]*arrow.Schema
	messageStream chan sources.MessageEvent
}

func NewKafkaSourcePlugin(config Config, schema []schema.StreamSchema) sources.DataSource {
	return &SourcePlugin{
		ctx:           context.Background(),
		config:        config,
		stream:        schema[0].StreamName,
		inputSchema:   schema,
		outputSchema:  sources.BuildOutputSchema(schema),
		messageStream: make(chan sources.MessageEvent),
	}
}

func (p *SourcePlugin) Connect(ctx context.Context) error {
	client, err := kgo.NewClient(p.GetConfig()...)

	if err != nil {
		panic(err)
	}

	err = client.Ping(p.ctx)

	if err != nil {
		return err
	}

	p.client = client

	return nil
}

func (p *SourcePlugin) Start() {
	go func(c *kgo.Client) {
		fetches := p.client.PollFetches(p.ctx)

		if errs := fetches.Errors(); len(errs) > 0 {
			panic(fmt.Sprint(errs))
		}

		iter := fetches.RecordIter()

		for !iter.Done() {
			record := iter.Next()

			builder := array.NewRecordBuilder(memory.DefaultAllocator, p.outputSchema[record.Topic])

			err := json.Unmarshal(record.Value, &builder)

			if err != nil {
				panic(err)
			}

			mBytes, _ := builder.NewRecord().MarshalJSON()
			m := message.NewMessage(message.Insert, p.stream, mBytes)

			p.messageStream <- sources.MessageEvent{
				Message: m,
				Err:     nil,
			}
		}
	}(p.client)
}

func (p *SourcePlugin) Stop() {
	p.client.Close()
	p.ctx.Done()
}

func (p *SourcePlugin) Events() chan sources.MessageEvent {
	return p.messageStream
}

func (p *SourcePlugin) GetConfig() []kgo.Opt {
	topics := make([]string, 0, len(p.outputSchema))

	for topic := range p.outputSchema {
		topics = append(topics, topic)
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(p.config.Brokers...),
		kgo.ConsumerGroup(p.config.ConsumerGroup),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}

	if p.config.Sasl {
		if p.config.SaslMechanism == "" || p.config.SaslUser == "" || p.config.SaslPassword == "" {
			panic("sasl param must be specified")
		}
		method := strings.ToLower(p.config.SaslMechanism)
		method = strings.ReplaceAll(method, "-", "")
		method = strings.ReplaceAll(method, "_", "")
		switch method {
		case "plain":
			opts = append(opts, kgo.SASL(plain.Auth{
				User: p.config.SaslUser,
				Pass: p.config.SaslPassword,
			}.AsMechanism()))
		case "scramsha256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: p.config.SaslUser,
				Pass: p.config.SaslPassword,
			}.AsSha256Mechanism()))
		case "scramsha512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: p.config.SaslUser,
				Pass: p.config.SaslPassword,
			}.AsSha512Mechanism()))
		case "awsmskiam":
			opts = append(opts, kgo.SASL(aws.Auth{
				AccessKey: p.config.SaslUser,
				SecretKey: p.config.SaslPassword,
			}.AsManagedStreamingIAMMechanism()))
		default:
			panic(fmt.Sprintf("unrecognized sasl option %s", p.config.SaslMechanism))
		}
	}

	return opts
}
