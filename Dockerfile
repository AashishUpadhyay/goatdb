FROM golang:1.17-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o goatdb ./main.go

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/goatdb .

EXPOSE 9999

CMD ["./goatdb"]