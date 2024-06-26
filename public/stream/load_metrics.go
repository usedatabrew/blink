package stream

import (
	"github.com/usedatabrew/blink/config"
	"github.com/usedatabrew/blink/internal/metrics"
	"github.com/usedatabrew/blink/internal/metrics/influx"
	"github.com/usedatabrew/blink/internal/metrics/prometheus"
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

func loadPrometheusMetrics(baseCfg config.Configuration) (metrics.Metrics, error) {
	var targetConfig prometheus.Config
	targetConfig.PipelineId = int(baseCfg.Service.PipelineId)
	targetConfig.GroupName = "local-worker-group"

	return prometheus.NewPlugin(targetConfig)
}
