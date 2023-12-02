package prometheus

import (
	"fmt"
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

	procCounters map[string][]prometheus.Counter

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
		procCounters: map[string][]prometheus.Counter{},
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

func (p *Plugin) RegisterProcessors(processors []string) {
	for _, proc := range processors {
		p.procCounters[proc] = []prometheus.Counter{
			promauto.NewCounter(prometheus.CounterOpts{
				Name: fmt.Sprintf("%s_execution_time", proc),
				Help: "Time taken to process message",
			}),
			promauto.NewCounter(prometheus.CounterOpts{
				Name: fmt.Sprintf("%s_dropped_messages", proc),
				Help: "Messages that were dropped, filtered by the processor",
			}),
			promauto.NewCounter(prometheus.CounterOpts{
				Name: fmt.Sprintf("%s_sent_messages", proc),
				Help: "The total number of messages send to the next sink/processor plugin",
			}),
			promauto.NewCounter(prometheus.CounterOpts{
				Name: fmt.Sprintf("%s_received_messages", proc),
				Help: "The total number of messages send to the sink plugin",
			}),
		}
	}
}

func (p *Plugin) SetProcessorExecutionTime(proc string, time int) {
	p.procCounters[proc][0].Inc()
}

func (p *Plugin) IncrementProcessorDroppedMessages(proc string) {
	p.procCounters[proc][1].Inc()
}

func (p *Plugin) IncrementProcessorReceivedMessages(proc string) {
	p.procCounters[proc][2].Inc()
}

func (p *Plugin) IncrementProcessorSentMessages(proc string) {
	p.procCounters[proc][3].Inc()
}
