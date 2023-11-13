package stream

import "lunaflow/internal/sources"

// SourceLoader wraps plan producer plugin in order to
// measure performance, build proper configuration and control the context
type SourceLoader struct {
	producerPlugin sources.DataSource
}

func newProducerLoader(pluginType string) {}

func (p *SourceLoader) Load() {}
func (p *SourceLoader) Events() (stream chan sources.MessageEvent) {
	for {
		select {
		case event := <-p.producerPlugin.Events():
			stream <- event
		}
	}
}
