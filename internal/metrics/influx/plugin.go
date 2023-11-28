package influx

import (
	"context"
	"strconv"
	"time"

	influxdb3 "github.com/InfluxCommunity/influxdb3-go/influxdb3"
	"github.com/rcrowley/go-metrics"
)

type Plugin struct {
	sentCounter         metrics.Counter
	receivedCounter     metrics.Counter
	sinkErrorsCounter   metrics.Counter
	sourceErrorsCounter metrics.Counter

	client       *influxdb3.Client
	writeOptions influxdb3.WriteOptions

	groupName  string
	pipelineId int
	orgId      string
	bucket     string
}

func NewPlugin(config Config) (*Plugin, error) {
	plugin := &Plugin{
		groupName:           config.GroupName,
		pipelineId:          config.PipelineId,
		orgId:               config.Org,
		bucket:              config.Bucket,
		sentCounter:         metrics.NewCounter(),
		receivedCounter:     metrics.NewCounter(),
		sinkErrorsCounter:   metrics.NewCounter(),
		sourceErrorsCounter: metrics.NewCounter(),
	}
	plugin.receivedCounter.Clear()
	plugin.sentCounter.Clear()
	plugin.sinkErrorsCounter.Clear()
	plugin.sourceErrorsCounter.Clear()

	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:  config.Host,
		Token: config.Token,
	})
	if err != nil {
		return nil, err
	}

	plugin.client = client
	plugin.writeOptions = influxdb3.WriteOptions{
		Database: config.Bucket,
	}

	go func() {
		for {
			time.Sleep(time.Second * 5)
			plugin.flushMetrics()
		}
	}()

	return plugin, nil
}

func (p *Plugin) IncrementReceivedCounter() {
	p.receivedCounter.Inc(1)
}

func (p *Plugin) IncrementSentCounter() {
	p.sentCounter.Inc(1)
}

func (p *Plugin) IncrementSinkErrCounter() {
	p.sinkErrorsCounter.Inc(1)
}

func (p *Plugin) IncrementSourceErrCounter() {
	p.sourceErrorsCounter.Inc(1)
}

func (p *Plugin) flushMetrics() {
	t := time.Now()

	point := influxdb3.NewPointWithMeasurement("blink_data").
		SetTag("group", p.groupName).
		SetTag("pipeline", strconv.Itoa(p.pipelineId)).
		SetField("sent_messages", p.sentCounter.Count()).
		SetTimestamp(t)

	if err := p.client.WritePointsWithOptions(context.Background(), &p.writeOptions, point); err != nil {
		panic(err)
	}

	point = influxdb3.NewPointWithMeasurement("blink_data").
		SetTag("group", p.groupName).
		SetTag("pipeline", strconv.Itoa(p.pipelineId)).
		SetField("received_messages", p.receivedCounter.Count()).
		SetTimestamp(t)

	if err := p.client.WritePointsWithOptions(context.Background(), &p.writeOptions, point); err != nil {
		panic(err)
	}

	point = influxdb3.NewPointWithMeasurement("blink_data").
		SetTag("group", p.groupName).
		SetTag("pipeline", strconv.Itoa(p.pipelineId)).
		SetField("sink_errors", p.sinkErrorsCounter.Count()).
		SetTimestamp(t)

	if err := p.client.WritePointsWithOptions(context.Background(), &p.writeOptions, point); err != nil {
		panic(err)
	}

	point = influxdb3.NewPointWithMeasurement("blink_data").
		SetTag("group", p.groupName).
		SetTag("pipeline", strconv.Itoa(p.pipelineId)).
		SetField("source_errors", p.sourceErrorsCounter.Count()).
		SetTimestamp(t)

	if err := p.client.WritePointsWithOptions(context.Background(), &p.writeOptions, point); err != nil {
		panic(err)
	}
}
