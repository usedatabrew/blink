package stream_context

import (
	"blink/internal/logger"
	"blink/internal/metrics"
	"context"
	"github.com/charmbracelet/log"
)

type Context struct {
	ctx     context.Context
	Metrics metrics.Metrics
	Logger  *log.Logger
}

func CreateContext() *Context {
	return &Context{
		ctx:     context.Background(),
		Metrics: nil,
		Logger:  logger.GetInstance(),
	}
}

func (c *Context) GetContext() context.Context {
	return c.ctx
}

func (c *Context) SetMetrics(mtr metrics.Metrics) {
	c.Metrics = mtr
}
