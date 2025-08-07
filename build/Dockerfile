FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOGC=off GOMEMLIMIT=28MiB go build -ldflags="-w -s" -o ./tmp/api ./cmd/api/main.go
RUN CGO_ENABLED=0 GOGC=off GOMEMLIMIT=28MiB go build -ldflags="-w -s" -o ./tmp/worker ./cmd/worker/main.go


FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/tmp/api .
COPY --from=builder /app/tmp/worker .

EXPOSE 9999