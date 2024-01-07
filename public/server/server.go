package server

import (
	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo/v4"
	"github.com/usedatabrew/blink/internal/stream_context"
	"github.com/usedatabrew/blink/internal/ui"
	"github.com/usedatabrew/blink/public/stream"
	"io"
	"net/http"
)

type Server struct {
	stream *stream.Stream
}

func CreateAndStartHttpServer(context *stream_context.Context, enableUi bool) {
	e := echo.New()

	e.GET("/metrics", echoprometheus.NewHandler())

	if enableUi {
		uiComponent := ui.NewUIComponent(e, context)
		uiComponent.RegisterHandlers()
	}

	err := e.Start(":3333")
	if err != nil {
		panic(err)
	}
}
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "Worker is up!")
}
