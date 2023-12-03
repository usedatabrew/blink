# <p align="center"> DataBrew - Blink </p>
### <p align="center"> OpenSource data streaming & processing engine to build event-driven systems </p>
<p align="center">
  <img src="./images/preview.png" width="150px" alt="Project social preview"> 
</p>
[![Create release](https://github.com/usedatabrew/blink/actions/workflows/release.yaml/badge.svg?branch=main)](https://github.com/usedatabrew/blink/actions/workflows/release.yaml)
![Latest version (latest semver)](https://img.shields.io/docker/v/usedatabrew/blink)

Blink ETL gives you the ability to create truly flexible data pipelines to implement even-driven architecture, database replication and data lakes

## Getting started
Let's give blink a try. Check out different options to start Blink on your local machine

### Run with Docker on Linux
Currently, we offer only linux-based arm64 build for docker
Create config with the name `blink.yaml` and run docker image

```shell
docker run -v blink.yaml:/blink.yaml usedatabrew/blink start
```

### Install Golang binary

```shell
go get -u github.com/usedatabrew/blink
```

```shell
blink start -c blink-config.yaml
```

### Install with Homebrew (MacOS)
