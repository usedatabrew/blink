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

	procMetrics              map[string][]metrics.Counter
	procExecutionTimeMetrics map[string]metrics.Gauge

	client       *influxdb3.Client
	writeOptions influxdb3.WriteOptions

	groupName  string
	pipelineId int
	orgId      string
	bucket     string
}

func NewPlugin(config Config) (*Plugin, error) {
	plugin := &Plugin{
		groupName:                config.GroupName,
		pipelineId:               config.PipelineId,
		orgId:                    config.Org,
		bucket:                   config.Bucket,
		sentCounter:              metrics.NewCounter(),
		receivedCounter:          metrics.NewCounter(),
		sinkErrorsCounter:        metrics.NewCounter(),
		sourceErrorsCounter:      metrics.NewCounter(),
		procMetrics:              map[string][]metrics.Counter{},
		procExecutionTimeMetrics: map[string]metrics.Gauge{},
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

func (p *Plugin) SetProcessorExecutionTime(proc string, time int64) {
	p.procExecutionTimeMetrics[proc].Update(time)
}

func (p *Plugin) IncrementProcessorDroppedMessages(proc string) {
	p.procMetrics[proc][0].Inc(0)
}

func (p *Plugin) IncrementProcessorReceivedMessages(proc string) {
	p.procMetrics[proc][0].Inc(1)
}

func (p *Plugin) IncrementProcessorSentMessages(proc string) {
	p.procMetrics[proc][0].Inc(2)
}

func (p *Plugin) RegisterProcessors(processors []string) {
	for _, proc := range processors {
		p.procExecutionTimeMetrics[proc] = metrics.NewGauge()
		p.procMetrics[proc] = []metrics.Counter{
			// dropped/filtered messages metrics
			metrics.NewCounter(),
			// sent messages metrics
			metrics.NewCounter(),
			// received metrics
			metrics.NewCounter(),
		}
	}
}

func (p *Plugin) flushMetrics() {
	t := time.Now()

	for proc, counters := range p.procMetrics {

		executionTimePoint := influxdb3.NewPointWithMeasurement("blink_data").
			SetTag("group", p.groupName).
			SetTag("pipeline", strconv.Itoa(p.pipelineId)).
			SetTag("processor", proc).
			SetField("execution_time", p.procExecutionTimeMetrics).
			SetTimestamp(t)

		if err := p.client.WritePointsWithOptions(context.Background(), &p.writeOptions, executionTimePoint); err != nil {
			panic(err)
		}

		filteredMessagesPoint := influxdb3.NewPointWithMeasurement("blink_data").
			SetTag("group", p.groupName).
			SetTag("pipeline", strconv.Itoa(p.pipelineId)).
			SetTag("processor", proc).
			SetField("dropped_messages", counters[0].Count()).
			SetTimestamp(t)

		if err := p.client.WritePointsWithOptions(context.Background(), &p.writeOptions, filteredMessagesPoint); err != nil {
			panic(err)
		}

		sentMessagesPoint := influxdb3.NewPointWithMeasurement("blink_data").
			SetTag("group", p.groupName).
			SetTag("pipeline", strconv.Itoa(p.pipelineId)).
			SetTag("processor", proc).
			SetField("sent_messages", counters[1].Count()).
			SetTimestamp(t)

		if err := p.client.WritePointsWithOptions(context.Background(), &p.writeOptions, sentMessagesPoint); err != nil {
			panic(err)
		}

		receivedMessagesPoint := influxdb3.NewPointWithMeasurement("blink_data").
			SetTag("group", p.groupName).
			SetTag("pipeline", strconv.Itoa(p.pipelineId)).
			SetTag("processor", proc).
			SetField("sent_messages", counters[2].Count()).
			SetTimestamp(t)

		if err := p.client.WritePointsWithOptions(context.Background(), &p.writeOptions, receivedMessagesPoint); err != nil {
			panic(err)
		}
	}

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
