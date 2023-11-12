package main

import (
	"fmt"
	"github.com/usedatabrew/pglogicalstream"
	"lunaflow/internal/sinks"
	luna_kafka "lunaflow/internal/sinks/kafka"
	"lunaflow/internal/sources/postgres_cdc"
	"lunaflow/public/service"
	"net/http"
)

func main() {
	s, err := service.InitService()
	if err != nil {
		panic(err)
	}
	if err = s.SetProducer(postgres_cdc.NewPostgresSourcePlugin(postgres_cdc.Config{
		Host:     "databrew-testing-instance.postgres.database.azure.com",
		Port:     5432,
		Database: "mocks",
		User:     "postgres",
		Schema:   "public",
		Password: "Lorem123",
		TablesSchema: []pglogicalstream.DbTablesSchema{
			{
				Table: "public.flights",
				Columns: []pglogicalstream.DbSchemaColumn{
					{
						Name:         "flight_id",
						DatabrewType: "Int64",
						Nullable:     false,
					},
				},
			},
		},
		SSLRequired:    true,
		StreamSnapshot: true,
	})); err != nil {
		panic(err)
	}

	kafkaSink := luna_kafka.NewKafkaSinkPlugin(luna_kafka.Config{
		Brokers:           []string{"pkc-ygz0p.norwayeast.azure.confluent.cloud:9092"},
		Sasl:              true,
		SaslPassword:      "stm1jZufR0njS4hVHnYgX4zrziADac/BZUGv2qh7Z0RBU3alrNTbxdMDob0p0aLg",
		SaslUser:          "2BUOXTEG5AKL4IEC",
		SaslMechanism:     "PLAIN",
		BindTopicToStream: false,
		TopicName:         "llsobgnp-default",
	})

	err = s.SetSinks([]sinks.DataSink{
		kafkaSink,
	})
	if err != nil {
		panic(err)
	}

	go func() {
		http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
			fmt.Fprintf(writer, "Hello")
		})

		http.ListenAndServe(":8080", nil)
	}()

	if err = s.Start(); err != nil {
		panic(err)
	}
}
