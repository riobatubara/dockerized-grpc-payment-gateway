package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	authpb "dockerized-grpc-payment-gateway/proto/auth"
	paymentpb "dockerized-grpc-payment-gateway/proto/payment"
)

// TransactionJob wraps an incoming request and its communication channels
type TransactionJob struct {
	ctx        context.Context
	request    *paymentpb.PaymentRequest
	resultChan chan *paymentpb.PaymentResponse
	errChan    chan error
}

type paymentServer struct {
	paymentpb.UnimplementedPaymentServiceServer
	jobQueue chan TransactionJob // Thread-safe queue channel
}

// 1. gRPC Interceptor (Middleware): Validates metadata tokens
func AuthUnaryInterceptor(authClient authpb.AuthServiceClient) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "metadata headers are missing")
		}

		tokens := md.Get("authorization")
		if len(tokens) == 0 || tokens[0] == "" {
			return nil, status.Error(codes.Unauthenticated, "authorization token is missing")
		}
		tokenString := tokens[0] // Correctly reading index 0 string fragment

		// Create a clean background context for internal communication
		authCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		authRes, err := authClient.Authorize(authCtx, &authpb.AuthorizeRequest{
			Token:        tokenString,
			RequiredRole: "admin", // Match the database row string value exactly
		})

		if err != nil {
			log.Printf("[Middleware Error] gRPC dial call to Auth Service failed: %v", err)
			return nil, status.Error(codes.Internal, "security authorization check failed")
		}

		if !authRes.Authorized {
			return nil, status.Error(codes.Unauthenticated, "access denied: insufficient permissions")
		}

		return handler(ctx, req)
	}
}

// 2. Asynchronous Worker Pool: Background Goroutines executing payment operations
func transactionWorker(workerID int, jobs <-chan TransactionJob) {
	for job := range jobs {
		// Implement structured Context checks to prevent handling abandoned client timeouts
		select {
		case <-job.ctx.Done():
			job.errChan <- status.Error(codes.DeadlineExceeded, "request timed out inside execution queue")
			continue
		default:
			// Simulate communicating with a third-party credit card gateway
			time.Sleep(200 * time.Millisecond)

			txID := fmt.Sprintf("tx_id_w%d_%d", workerID, time.Now().UnixNano())

			// Send response back using individual response channels
			job.resultChan <- &paymentpb.PaymentResponse{
				TransactionId: txID,
				Status:        "SUCCESSFUL_SETTLEMENT",
			}
		}
	}
}

// 3. Core Service Method:: Handled purely by passing payload to background threads
func (s *paymentServer) ProcessPayment(ctx context.Context, req *paymentpb.PaymentRequest) (*paymentpb.PaymentResponse, error) {
	// Establish a strict overall transaction window processing timeout (3 seconds)
	txCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	// Instantiate isolated structural synchronization return channels
	resChan := make(chan *paymentpb.PaymentResponse, 1)
	errChan := make(chan error, 1)

	job := TransactionJob{
		ctx:        txCtx,
		request:    req,
		resultChan: resChan,
		errChan:    errChan,
	}

	// Dispatch request directly to background worker pool channel queue
	select {
	case s.jobQueue <- job:
		// Job successfully added to queue buffer pipeline
	case <-txCtx.Done():
		return nil, status.Error(codes.ResourceExhausted, "payment gateway queue is full")
	}

	// Dynamic Multiplexing await logic loop using select statement blocking blocks
	select {
	case result := <-resChan:
		return result, nil
	case err := <-errChan:
		return nil, err
	case <-txCtx.Done():
		return nil, status.Error(codes.DeadlineExceeded, "upstream latency processing timeout exceeded")
	}
}

func main() {
	port := os.Getenv("PAYMENT_PORT")
	authAddr := os.Getenv("AUTH_SERVICE_ADDR")

	// 1. Establish insecure network client connection channel to Auth cluster node
	conn, err := grpc.NewClient(authAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Critical: Could not connect to auth cluster link: %v", err)
	}
	defer conn.Close()
	authClient := authpb.NewAuthServiceClient(conn)

	// 2. Initialize Buffered Concurrency Job Channels
	queueSize := 500
	jobQueue := make(chan TransactionJob, queueSize)

	// 3. Spawn Worker Pools using multiple Goroutines
	workerPoolSize := 5
	for w := 1; w <= workerPoolSize; w++ {
		go transactionWorker(w, jobQueue)
	}

	// 4. Bind and Listen on TCP Port
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Critical: Failed to monitor payment port %s: %v", port, err)
	}

	// 5. Register gRPC server containing the Middleware Interceptor handler
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(AuthUnaryInterceptor(authClient)),
	)
	paymentpb.RegisterPaymentServiceServer(grpcServer, &paymentServer{jobQueue: jobQueue})

	log.Printf("SUCCESS: Payment Service Engine operational on port: %s", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Critical: Failed to boot Payment gRPC: %v", err)
	}
}
