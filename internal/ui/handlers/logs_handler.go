package handlers

import (
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"time"
)

var (
	upgrader = websocket.Upgrader{}
)

type LogHandler struct {
}

func NewLogHandler() Handler {
	return LogHandler{}
}

func (l LogHandler) SelfRegister(e *echo.Echo) {
	e.GET("/api/logs", func(c echo.Context) error {
		ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
		if err != nil {
			return err
		}
		defer ws.Close()

		for {
			// Write
			err := ws.WriteMessage(websocket.TextMessage, []byte("Hello, Client!"))
			if err != nil {
				break
			}
			time.Sleep(time.Second * 2)
		}

		return nil
	})
}
