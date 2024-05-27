# <p align="center"> DataBrew - Blink </p>

### <p align="center"> OpenSource data streaming & processing engine to build event-driven systems </p>

<p align="center">
  <img src="./images/preview.png" width="150px" alt="Project social preview">
</p>

---

[![Build on push to main](https://github.com/usedatabrew/blink/actions/workflows/build_main.yaml/badge.svg)](https://github.com/usedatabrew/blink/actions/workflows/build_main.yaml)
![Latest version (latest semver)](https://img.shields.io/docker/v/usedatabrew/blink)

Blink gives your ability to create event driven systems by adopting CDC or enabling integration with third-party systems like
AirTable or stripe to stream your data directly to your systems. 

# Table of Contents

1. [Installation](#getting-started)
2. [Running Blink locally](#running-blink-locally)
3. [Docs](https://docs.databrew.tech/get-started-with-open-source.html)

## Getting started

Let's give Blink a try. Check out different options to start Blink on your local machine

### Run with Docker on Linux

Currently, we offer only Linux-based arm64 build for docker
Create a config with the name `blink.yaml` and run the docker image

```shell
docker run -v ./blink.yaml:/app/blink.yaml usedatabrew/blink start
```

### Install Golang binary

![Latest version (latest semver)](https://img.shields.io/docker/v/usedatabrew/blink)

```shell
go install github.com/usedatabrew/blink@v1.16.9
```

### Running Blink locally

To run Blink locally - you have to create a config file that will define streams

```yaml
service:
  pipeline_id: 1
source:
  driver: playground
  config:
    data_type: market
    publish_interval: 1
    historical_batch: false
  stream_schema:
    - stream: market
      columns:
        - name: company
          nativeConnectorType: String
          databrewType: String
          nullable: false
          pk: false
        - name: currency
          nativeConnectorType: String
          databrewType: String
          nullable: false
          pk: false
processors:
  - driver: sql
    config:
      query: "select * from streams.market where currency = 'USD'"
sink:
  driver: stdout
  config: {}
```

```shell
blink start -c blink-config.yaml
```
