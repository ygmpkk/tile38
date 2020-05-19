FROM golang:1.13 as builder

# Copy local code to the container image.
WORKDIR /tile38
COPY . .
COPY ./cmd/tile38-server/main.go .

# Build the command inside the container.
# (You may fetch or manage dependencies here,
# either manually or with a tool like "godep".)
RUN CGO_ENABLED=0 GOOS=linux go build -v -o tile38-server

FROM alpine:3.8
RUN apk add --no-cache ca-certificates

COPY --from=builder /tile38/tile38-server /usr/local/bin/tile38-server
#ADD tile38-cli /usr/local/bin
#ADD tile38-benchmark /usr/local/bin

RUN addgroup -S tile38 && \
    adduser -S -G tile38 tile38 && \
    mkdir /data && chown tile38:tile38 /data

VOLUME /data

EXPOSE 9851
CMD ["tile38-server", "-d", "/data"]
