FROM golang:1.21-alpine as build

WORKDIR /app

COPY . .

RUN go mod tidy

RUN cd cmd/blink && go build

FROM alpine as certs
RUN apk update && apk add ca-certificates
RUN apk add mysql-client

FROM busybox

WORKDIR /app

COPY --from=certs /etc/ssl/certs /etc/ssl/certs
COPY --from=build /app/cmd/blink/blink /app/

ENV GOMAXPROCS=2

ENTRYPOINT ["/app/blink"]

CMD ["/app/blink"]