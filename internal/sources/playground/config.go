package playground

type Config struct {
	DataType               string `json:"data_type" yaml:"data_type"`
	PublishIntervalSeconds int64  `json:"publish_interval" yaml:"publish_interval"`
	// Historical batch will produce 1k messages before starting the interval
	HistoricalBatch bool `json:"historical_batch" yaml:"historical_batch"`
}
