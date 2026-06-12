# --- Stage 1: Build Environment ---
FROM golang:1.21-alpine AS builder

# Install dependencies required for compiling CGO bindings (librdkafka)
RUN apk add --no-cache build-base pkgconfig librdkafka-dev

WORKDIR /app

# Copy dependency manifests first to leverage Docker layer caching
COPY services/search-ingest-agent/go.mod ./
RUN go mod download

# Copy the agent application code and shared packages if required
COPY services/search-ingest-agent/ ./services/search-ingest-agent/

# Compile the Go binary with CGO enabled for librdkafka performance optimization
WORKDIR /app/services/search-ingest-agent
RUN CGO_ENABLED=1 GOOS=linux go build -tags musl -ldflags="-w -s" -o search-ingest-agent cmd/main.go

# --- Stage 2: Minimal Runtime ---
FROM alpine:3.19

# Install runtime security certs and shared native libraries
RUN apk add --no-cache ca-certificates librdkafka

WORKDIR /root/

# Copy the compiled executable from the builder stage
COPY --from=builder /app/services/search-ingest-agent/search-ingest-agent .

# Create directory placeholder for mTLS keystores / truststores
RUN mkdir -p /var/private/ssl

EXPOSE 8080

CMD ["./search-ingest-agent"]