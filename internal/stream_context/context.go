package stream_context

import (
	"context"
	"github.com/charmbracelet/log"
	"github.com/usedatabrew/blink/internal/logger"
	"github.com/usedatabrew/blink/internal/metrics"
	"github.com/usedatabrew/blink/internal/offset_storage"
	"sync"
)

var once sync.Once

type Context struct {
	pipelineId    int64
	ctx           context.Context
	Metrics       metrics.Metrics
	Logger        *log.Logger
	offsetStorage offset_storage.OffsetStorage
}

func CreateContext(pipelineId int64) *Context {
	return &Context{
		pipelineId: pipelineId,
		ctx:        context.Background(),
		Metrics:    nil,
		Logger:     logger.GetInstance(),
	}
}

func (c *Context) GetContext() context.Context {
	return c.ctx
}

func (c *Context) SetMetrics(mtr metrics.Metrics) {
	c.Metrics = mtr
}

func (c *Context) PipelineId() int64 {
	return c.pipelineId
}

func (c *Context) SetOffsetStorage(ofsSt offset_storage.OffsetStorage) {
	c.offsetStorage = ofsSt
}

func (c *Context) OffsetStorage() offset_storage.OffsetStorage {
	return c.offsetStorage
}
