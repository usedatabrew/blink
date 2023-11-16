package main

import (
	"astro/public/stream"
	"fmt"
	"github.com/charmbracelet/log"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
	"net/http"
	"os"
	"runtime"
	"time"
)

func monitorGoroutines() {
	for {
		numGoroutines := runtime.NumGoroutine()
		fmt.Println("Number of Goroutines:", numGoroutines)
		time.Sleep(time.Second) // Adjust the interval as needed
	}
}

func main() {
	rules := []tracer.SamplingRule{tracer.RateRule(1)}
	tracer.Start(
		tracer.WithSamplingRules(rules),
		tracer.WithService("astro"),
		tracer.WithEnv("local"),
	)
	defer tracer.Stop()

	err := profiler.Start(
		profiler.WithService("astro"),
		profiler.WithEnv("local"),
		profiler.WithProfileTypes(
			profiler.CPUProfile,
			profiler.HeapProfile,

			// The profiles below are disabled by
			// default to keep overhead low, but
			// can be enabled as needed.
			profiler.BlockProfile,
			profiler.MutexProfile,
			profiler.GoroutineProfile,
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer profiler.Stop()
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

	go monitorGoroutines()

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
