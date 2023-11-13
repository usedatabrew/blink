package stream_context

import (
	"astro/internal/metrics"
	"context"
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
