package metrics

type Metrics interface {
	IncrementReceivedCounter()
	IncrementSentCounter()
	IncrementSourceErrCounter()
	IncrementSinkErrCounter()
}
