FROM golang:1.15 AS builder

WORKDIR /redis-eta-listener

RUN echo "hosts: files dns" > /etc/nsswitch.conf.min

RUN echo "nobody:x:65534:65534:Nobody:/:" > /etc/passwd.min

RUN apt-get update && \
    apt-get install -y ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 \
    go build -ldflags '-s -w' -tags 'osusergo netgo' redis-eta-listener

FROM scratch AS final

# Copy over the binary artifact
COPY --from=builder /redis-eta-listener/redis-eta-listener /

# Copy configuration from builder
COPY --from=builder /etc/nsswitch.conf.min /etc/nsswitch.conf
COPY --from=builder /etc/passwd.min /etc/passwd
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

USER nobody

ENTRYPOINT ["/redis-eta-listener"]