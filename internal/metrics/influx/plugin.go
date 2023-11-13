package influx

import (
	"context"
	"errors"
	"fmt"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/rcrowley/go-metrics"
	"time"
)

type Plugin struct {
	sentCounter         metrics.Counter
	receivedCounter     metrics.Counter
	sinkErrorsCounter   metrics.Counter
	sourceErrorsCounter metrics.Counter

	writer api.WriteAPI
	client influxdb2.Client

	groupName  string
	pipelineId int
}

func NewPlugin(config Config) (*Plugin, error) {
	plugin := &Plugin{
		groupName:  config.GroupName,
		pipelineId: config.PipelineId,
	}

	plugin.client = influxdb2.NewClient(config.Url, config.Token)

	if ok, err := plugin.client.Ready(context.TODO()); ok == nil || err != nil {
		return nil, errors.New("metrics connection failed")
	}

	writer := plugin.client.WriteAPI(config.Org, config.Bucket)
	plugin.writer = writer

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
	point := influxdb2.NewPoint(
		"stream_data",
		map[string]string{
			"group":    p.groupName,
			"pipeline": fmt.Sprintf("%d", p.pipelineId),
		},
		map[string]interface{}{
			"received_messages": p.receivedCounter.Count(),
			"sent_messages":     p.sentCounter.Count(),
			"sink_errors":       p.sinkErrorsCounter.Count(),
			"source_errors":     p.sourceErrorsCounter.Count(),
		},
		time.Now(),
	)

	// Write the point to InfluxDB
	p.writer.WritePoint(point)
}
