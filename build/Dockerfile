FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOGC=off GOMEMLIMIT=50MiB go build -ldflags="-w -s" -o ./tmp/main ./cmd/api/main.go

FROM alpine:latest

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

WORKDIR /app

COPY --from=builder /app/tmp/main .

EXPOSE 9999

CMD ["./main"]