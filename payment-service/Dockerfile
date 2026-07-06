FROM golang:1.25-alpine AS builder

RUN apk add --no-cache build-base musl-dev

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -tags musl -o payment-binary ./payment-service/main.go

FROM alpine:latest

RUN apk add --no-cache libc6-compat

WORKDIR /root/
COPY --from=builder /app/payment-binary .

CMD ["./payment-binary"]
