package main

import (
	"astro/public/stream"
	"fmt"
	"net/http"
	"os"
)

func main() {
	configFile, err := os.ReadFile("./config/example.yaml")
	if err != nil {
		panic(err)
	}

	var streamService *stream.Stream
	serviceConfiguration, err := stream.ReadInitConfigFromYaml(configFile)
	streamService, err = stream.InitFromConfig(serviceConfiguration)
	if err != nil {
		panic(err)
	}

	go func() {
		http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
			fmt.Fprintf(writer, "Hello")
		})

		http.ListenAndServe(":8080", nil)
	}()

	if err = streamService.Start(); err != nil {
		panic(err)
	}
}
