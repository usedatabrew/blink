package metrics

type Metrics interface {
	IncrementReceivedCounter()
	IncrementSentCounter()
	IncrementSourceErrCounter()
	IncrementSinkErrCounter()

	RegisterProcessors(processors []string)

	SetProcessorExecutionTime(proc string, time int)
	IncrementProcessorDroppedMessages(proc string)
	IncrementProcessorReceivedMessages(proc string)
	IncrementProcessorSentMessages(proc string)
}
