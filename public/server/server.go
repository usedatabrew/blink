package server

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/usedatabrew/blink/public/stream"
	"io"
	"net/http"
)

type Server struct {
	stream *stream.Stream
}

func CreateAndStartHttpServer(stream *stream.Stream) {
	server := &Server{}
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", server.status)
	err := http.ListenAndServe(":3333", nil)
	if err != nil {
		panic(err)
	}
}
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "Worker is up!")
}
