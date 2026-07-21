# syntax=docker/dockerfile:1

# ----- build stage -----
FROM golang:1.22-alpine AS build
WORKDIR /src

# Cache module downloads first.
COPY go.mod go.sum* ./
RUN go mod download

COPY . .

# Build both binaries into /out.
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/agora-coordinator ./cmd/coordinator && \
    CGO_ENABLED=0 GOOS=linux go build -o /out/agora-worker ./cmd/worker

# ----- runtime stage -----
FROM alpine:3.20
WORKDIR /app

# Non-root runtime user.
RUN addgroup -S agora && adduser -S agora -G agora

COPY --from=build /out/agora-coordinator /usr/local/bin/agora-coordinator
COPY --from=build /out/agora-worker /usr/local/bin/agora-worker
COPY data/ /app/data/

USER agora

# Default entrypoint is overridden per-service in docker-compose.
ENTRYPOINT ["/usr/local/bin/agora-coordinator"]
