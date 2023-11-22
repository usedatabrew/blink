package prometheus

import (
	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Plugin struct {
	sentCounter         prometheus.Counter
	receivedCounter     prometheus.Counter
	sinkErrorsCounter   prometheus.Counter
	sourceErrorsCounter prometheus.Counter

	writeOptions influxdb3.WriteOptions

	groupName  string
	pipelineId int
}

func NewPlugin(config Config) (*Plugin, error) {
	plugin := &Plugin{
		groupName:  config.GroupName,
		pipelineId: config.PipelineId,
		sentCounter: promauto.NewCounter(prometheus.CounterOpts{
			Name: "sent_messages",
			Help: "The total number of messages send to the sink plugin",
		}),
		receivedCounter: promauto.NewCounter(prometheus.CounterOpts{
			Name: "received_messages",
			Help: "The total number of messages received by source plugin",
		}),
		sinkErrorsCounter: promauto.NewCounter(prometheus.CounterOpts{
			Name: "sink_errors",
			Help: "The total number of messages that were failed to be sent to sink",
		}),
		sourceErrorsCounter: promauto.NewCounter(prometheus.CounterOpts{
			Name: "source_errors",
			Help: "The total number of errors from source connector",
		}),
	}
	return plugin, nil
}

func (p *Plugin) IncrementReceivedCounter() {
	p.receivedCounter.Inc()
}

func (p *Plugin) IncrementSentCounter() {
	p.sentCounter.Inc()
}

func (p *Plugin) IncrementSinkErrCounter() {
	p.sinkErrorsCounter.Inc()
}

func (p *Plugin) IncrementSourceErrCounter() {
	p.sourceErrorsCounter.Inc()
}
