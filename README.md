# dockerized-grpc-payment-gateway

### If your machine does not have protobuf-compiler
```
sudo apt update
sudo apt install -y protobuf-compiler
```

### gRPC packages
```
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### Compile Auth Definitions
```
protoc \
  --go_out=. \
  --go-grpc_out=. \
  proto/auth.proto
```

### Compile Payment Definitions
```
protoc \
  --go_out=. \
  --go-grpc_out=. \
  proto/payment.proto
```

### Compile Both Auth & Payment Definitions
```
protoc \
  --go_out=. \
  --go-grpc_out=. \
  proto/auth.proto proto/payment.proto
```

### Build both auth-service & payment service Dockerfile
```
docker compose --env-file .env up --build -d
```

### Deploy all service without build
```
docker compose --env-file .env up -d
```

### Check broadcast
```
docker exec -it gateway_kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic payment_processed \
  --from-beginning
```


### Running test
```
go test -v client_test.go
```

```
docker exec -it gateway_postgres psql -U user_123 -d payment_db -c "SELECT id, username, role FROM users;"
```


<!-- dockerized-grpc-payment-gateway/
├── proto/           
│   ├── auth/
│   │   ├── auth.pb.go
│   │   └── auth_grpc.pb.go
│   ├── payment/
│   │   ├── payment.pb.go
│   │   └── payment_grpc.pb.go
│   ├── auth.proto    
│   └── payment.proto 
├── auth-service/
│   └── main.go
└── payment-service/
│   └── main.go
├── .env
├── go.mod
├── go.sum
├── init.sql
├── docker-compose.yml -->