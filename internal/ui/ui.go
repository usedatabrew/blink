package ui

import (
	"github.com/labstack/echo/v4"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/blink/internal/ui/handlers"
)

type UI struct {
	streamContext *stream_context.Context
	echo          *echo.Echo
}

func NewUIComponent(e *echo.Echo, streamContext *stream_context.Context) *UI {
	return &UI{
		streamContext: streamContext,
		echo:          e,
	}
}

func (u *UI) RegisterHandlers() {
	u.echo.Static("/ui", "./internal/ui/static")
	handlers.NewLogHandler().SelfRegister(u.echo)
}
