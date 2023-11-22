package main

import (
	"astro/public/server"
	"astro/public/stream"
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

	go server.CreateAndStartHttpServer(streamService)
	if err = streamService.Start(); err != nil {
		panic(err)
	}
}
