# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod ./
RUN go mod download

# Copy source code
COPY . .

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /node ./cmd/node

# Runtime stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy binary from builder
COPY --from=builder /node /app/node

# Expose default port
EXPOSE 9000

# Run the node
ENTRYPOINT ["/app/node"]
CMD ["-address", "0.0.0.0", "-port", "9000"]
