FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.22

RUN adduser -D -u 10001 raftkv && mkdir -p /data && chown raftkv:raftkv /data
COPY --from=build /out/raftkv /out/raftkvctl /usr/local/bin/

USER raftkv
VOLUME /data
EXPOSE 9001 50051

HEALTHCHECK --interval=3s --timeout=2s --start-period=2s --retries=10 \
  CMD raftkvctl -addr 127.0.0.1:50051 status > /dev/null

ENTRYPOINT ["raftkv"]
