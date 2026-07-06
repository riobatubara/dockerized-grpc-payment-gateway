package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "dockerized-grpc-payment-gateway/proto/auth"
)

// Define our gRPC Server structure
type authServer struct {
	pb.UnimplementedAuthServiceServer
	db  *sql.DB
	rdb *redis.Client
}

// 1. Login Method: Checks Postgres and stores session in Redis
func (s *authServer) Login(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResponse, error) {
	// Use Context with a clear database timeout limit (3 seconds)
	dbCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var userID int
	var passwordHash string
	var userRole string

	// Query Postgres to look for the username
	query := "SELECT id, password_hash, role FROM users WHERE username = $1"
	if err := s.db.QueryRowContext(dbCtx, query, req.Username).Scan(&userID, &passwordHash, &userRole); err != nil {
		log.Printf("Postgres Error: %v", err)
		return nil, status.Error(codes.Internal, "database error")
	} else if err == sql.ErrNoRows {
		return nil, status.Error(codes.Unauthenticated, "invalid username or password")
	}

	if err := bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(req.Password)); err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid username or password")
	}

	// Generate a unique session token string
	sessionToken := fmt.Sprintf("session_token_%d_%d", userID, time.Now().UnixNano())

	// Store the sessionToken -> userRole string mapping in Redis with a 1-hour expiration
	redisCtx, redisCancel := context.WithTimeout(ctx, 2*time.Second)
	defer redisCancel()

	if err := s.rdb.Set(redisCtx, sessionToken, userRole, 1*time.Hour).Err(); err != nil {
		log.Printf("Redis Error: %v", err)
		return nil, status.Error(codes.Internal, "failed to create user session cache")
	}

	return &pb.LoginResponse{Token: sessionToken}, nil
}

// 2. Authorize Method: Validates the token and verifies user role
func (s *authServer) Authorize(ctx context.Context, req *pb.AuthorizeRequest) (*pb.AuthorizeResponse, error) {
	redisCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	// Fetch the role attached to this token from Redis cache
	userRole, err := s.rdb.Get(redisCtx, req.Token).Result()
	if err == redis.Nil {
		// token not found / expired
		return &pb.AuthorizeResponse{Authorized: false}, nil
	} else if err != nil {
		log.Printf("Redis Error: %v", err)
		return nil, status.Error(codes.Internal, "cache validation error")
	}

	// Verify if the active role matches the permission requested by the payment-service
	if userRole != req.RequiredRole {
		return &pb.AuthorizeResponse{Authorized: false}, nil
	}

	return &pb.AuthorizeResponse{Authorized: true, UserId: "user_verified"}, nil
}

func main() {
	// Extract our configuration settings directly from environment setup variables
	port := os.Getenv("AUTH_PORT")
	dbURL := os.Getenv("DATABASE_URL")
	redisAddr := os.Getenv("REDIS_ADDR")

	// 1. Initialize Postgres SQL connection pool
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Critical: Could not connect to Postgres: %v", err)
	}
	defer db.Close()

	// 2. Initialize Redis Client
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer rdb.Close()

	// 3. Set up the TCP network listener bind
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Critical: Failed to listen on port %s: %v", port, err)
	}

	// 4. Register and boot our gRPC server cluster instance
	grpcServer := grpc.NewServer()
	pb.RegisterAuthServiceServer(grpcServer, &authServer{db: db, rdb: rdb})

	log.Printf("SUCCESS: Auth Service running securely on port: %s", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Critical: Failed to serve gRPC: %v", err)
	}
}
