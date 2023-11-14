package stream

import (
	"astro/internal/metrics"
	"astro/internal/metrics/influx"
	"gopkg.in/yaml.v3"
)

func loadInfluxMetrics(config interface{}) (metrics.Metrics, error) {
	marshaled, err := yaml.Marshal(config)
	if err != nil {

		panic("cant load influx")
	}
	var targetConfig influx.Config
	err = yaml.Unmarshal(marshaled, &targetConfig)
	return influx.NewPlugin(targetConfig)
}
