package stream

import (
  "github.com/usedatabrew/blink/config"
	"gopkg.in/yaml.v3"
	"log"
	"testing"
)

func TestReadConfigAsYaml(t *testing.T) {
	yamlData := `
service:
  enable_influx: true
  reload_on_restart: false

source:
  driver: postgres_cdc
  schema:
    - stream: public.flights
      columns:
        - name: flight_id
          databrewType: Int32
          nativeConnectorType: integer
          pk: true
          nullable: false
  config:
    host: databrew-testing-instance.postgres.database.azure.com
    slot_name: databrew_replication_slot_15_117
    user: postgres
    password: Lorem123
    port: 5432
    schema: public
    stream_snapshot: true
    snapshot_memory_safety_factor: 0.1
    snapshot_batch_size: 10000
    database: mocks
    tables:
      - flights

target:
  plugin: stdout
  config: {}
`

	var (
		cfg config.Configuration
	)

	// Unmarshal the YAML into the custom struct
	err := yaml.Unmarshal([]byte(yamlData), &cfg)
	if err != nil {
		log.Fatal(err)
	}

	if cfg.Source.Driver != "postgres_cdc" {
		t.Fatal("Invalid driver in config")
	}
}
