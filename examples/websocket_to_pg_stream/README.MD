## Stream BTC Price fluctuations from WebSocket to PostgreSQL

This example shows how to use start Blink with Docker
and how to configure it to consume data from WebSocket Gateway, perform simple filtering with SQL and apply changed
to PostgreSQL

### Blink configuration

```yaml
service:
  id: 132
  pipeline_id: 132
  stream_schema:
    - stream: coin_price
      columns:
        - name: coin
          databrewType: String
          pk: false
          nullable: false
        - name: price
          databrewType: Int64
          pk: false
          nullable: false

source:
  driver: websocket
  config:
    url: wss://databrew-ws-gateway.fly.dev/ws

processors:
  - driver: sql
    config:
      query: "select * from stream.coin_price where price > 800"
  #      query: "select price from stream.coin_price where price > 800"
  - driver: log
    config:
      stream: "*"

sink:
  driver: postgres
  config:
    host: postgres
    port: 5432
    user: postgres
    password: lorem123
    database: postgres
    schema: public
    ssl_required: false
```

### Docker compose config
```yaml
name: "blink_ws"

services:
  blink_instance:
    container_name: "blink"
    image: usedatabrew/blink:1.6.1
    ports:
      - 3333:3333
    volumes:
      - ./blink.yaml:/blink.yaml
    command: [ "start", "--config", "/blink.yaml" ]
    networks:
      - postgres
    depends_on:
      - postgres
  postgres:
    image: postgres:14-alpine
    ports:
      - 5432:5432
    volumes:
      - /var/lib/postgresql/data
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: lorem123
      POSTGRES_DB: postgres
    networks:
      - postgres

networks:
  postgres:
    driver: bridge
```

### Starting

```shell
docker-compose up
```

You should see the logs

```shell
blink                | 2024-01-02 15:58:51 INFO blink: Bootstrapping blink Stream-ETL
blink                | 2024-01-02 15:58:51 WARN Metrics: No influx config has been provided. Fallback to local prometheus metrics
blink                | 2024-01-02 15:58:51 INFO Metrics: Component has been loaded
blink                | 2024-01-02 15:58:51 INFO Source: Loaded driver=websocket
blink                | 2024-01-02 15:58:51 INFO Processors: Loaded driver=sql
blink                | 2024-01-02 15:58:51 INFO Processors: Loaded driver=log
blink                | 2024-01-02 15:58:51 INFO Sinks: Loaded driver=postgres
blink                | 2024/01/02 15:58:52 INFO PostgreSQL Sink: Generated init statements to create table for the sink database
blink                |   statements=
blink                |   │ [CREATE TABLE IF NOT EXISTS coin_price (
blink                |   │   coin text PRIMARY KEY NOT NULL,
blink                |   │   price bigint NOT NULL
blink                |   │ );]
blink                | 2024/01/02 15:58:52 INFO PostgreSQL Sink: Creating table for stream stream=coin_price
blink                | 2024/01/02 15:58:54 INFO log-processor: message received
blink                |   message=
blink                |   │ [{"coin":"BTC","price":842}
blink                |   │ ]
blink                | 2024/01/02 15:58:54 INFO PostgreSQL Sink: Applying operation op=insert stream=coin_price
blink                | 2024/01/02 15:58:55 INFO log-processor: message received
blink                |   message=
blink                |   │ [{"coin":"BTC","price":836}
blink                |   │ ]
blink                | 2024/01/02 15:58:55 INFO PostgreSQL Sink: Applying operation op=insert stream=coin_price
```
