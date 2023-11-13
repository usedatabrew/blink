package stream

import (
	"context"
	"lunaflow/internal/metrics"
)

type Context struct {
	ctx     context.Context
	Metrics metrics.Metrics
}

func CreateContext(mtr metrics.Metrics) *Context {
	return &Context{
		ctx:     context.Background(),
		Metrics: mtr,
	}
}
