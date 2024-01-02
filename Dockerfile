FROM golang:1.21-alpine as build

WORKDIR /app

COPY . .

RUN go mod tidy

RUN cd cmd/blink && go build

FROM busybox

WORKDIR /app

COPY --from=build /app/cmd/blink/blink /app/

ENV GOMAXPROCS=2

ENTRYPOINT ["/app/blink"]

CMD ["/app/blink"]