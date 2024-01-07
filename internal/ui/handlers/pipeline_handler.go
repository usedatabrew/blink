package handlers

import (
	"github.com/labstack/echo/v4"
	"github.com/usedatabrew/blink/internal/stream_context"
)

type PipelineHandler struct {
	streamCtx *stream_context.Context
}

func NewPipelineHandler(streamCtx *stream_context.Context) Handler {
	return PipelineHandler{streamCtx: streamCtx}
}

func (p PipelineHandler) SelfRegister(e *echo.Echo) {
	//TODO implement me
	panic("implement me")
}
