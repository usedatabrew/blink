FROM golang:1.21-bookworm

WORKDIR /app

COPY . .

RUN go mod tidy

RUN cd cmd/blink && go build blink/cmd/blink

ENV GOMAXPROCS=2

ENTRYPOINT ["./cmd/blink/blink"]

CMD ["./cmd/blink/blink"]