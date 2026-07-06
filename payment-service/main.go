package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	_ "github.com/lib/pq"
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

type SystemStats struct {
	mu       sync.Mutex
	Counters map[string]int // Thread-safe state registry
}

var Stats = &SystemStats{
	Counters: make(map[string]int),
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
func transactionWorker(workerID int, jobs <-chan TransactionJob, db *sql.DB, producer *kafka.Producer) {
	topic := "payment_processed"

	for job := range jobs {
		// Implement structured Context checks to prevent handling abandoned client timeouts
		select {
		case <-job.ctx.Done():
			job.errChan <- status.Error(codes.DeadlineExceeded, "request timed out inside execution queue")
			continue
		default:
			// 1. Core Transaction Execution
			time.Sleep(100 * time.Millisecond)

			txID := fmt.Sprintf("tx_id_w%d_%d", workerID, time.Now().UnixNano())

			// 2. Commit Transaction Row to DB
			dbCtx, dbCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, dbErr := db.ExecContext(dbCtx,
				"INSERT INTO transactions (id, user_id, amount, currency, status) VALUES ($1, $2, $3, $4, $5)",
				txID, 1, job.request.Amount, job.request.Currency, "COMPLETED",
			)
			dbCancel()
			if dbErr != nil {
				log.Printf("[WORKER %d] DB commit error: %v", workerID, dbErr)
			}

			// 3. Dispatch Message Event into Kafka Stream
			payload := fmt.Sprintf("Processed transaction: %s for %0.2f %s", txID, job.request.Amount, job.request.Currency)
			if err := producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key:            []byte(txID),
				Value:          []byte(payload),
			}, nil); err != nil {
				log.Printf("[WORKER %d] Confluent production enqueue fault: %v", workerID, err)
			} else {
				log.Printf("[WORKER %d] Message enqueued for event streaming: %s", workerID, txID)
			}
			producer.Flush(10)

			// 4. Safely Update Metrics Registry Map using sync.Mutex
			Stats.mu.Lock()
			Stats.Counters["total_processed_payments"]++
			Stats.Counters[fmt.Sprintf("worker_%d_jobs", workerID)]++
			log.Printf("[MUTEX STATUS] Total global processing score updated safely to: %d", Stats.Counters["total_processed_payments"])
			Stats.mu.Unlock()

			// Send response status back using response channels
			job.resultChan <- &paymentpb.PaymentResponse{
				TransactionId: txID,
				Status:        "SUCCESSFUL_SETTLEMENT",
			}
		}
	}
}

// 3. LISTEN FOR ASYNCHRONOUS KAFKA DELIVERY EVENTS
func listenToKafkaDeliveryReports(producer *kafka.Producer) {
	for e := range producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("[KAFKA COURIER] Transmission error: %v", ev.TopicPartition.Error)
			} else {
				log.Printf("[KAFKA COURIER] Confirmed network ack received on partition %d for key %s",
					ev.TopicPartition.Partition, string(ev.Key))
			}
		}
	}
}

// 4. Core Service Method:: Handled purely by passing payload to background threads
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
		return nil, status.Error(codes.DeadlineExceeded, "latency timeout error")
	}
}

func main() {
	port := os.Getenv("PAYMENT_PORT")
	authAddr := os.Getenv("AUTH_SERVICE_ADDR")
	dbURL := os.Getenv("DATABASE_URL")
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")

	// 1. Establish insecure network client connection channel to Auth cluster node
	conn, err := grpc.NewClient(authAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Critical: Could not connect to auth cluster link: %v", err)
	}
	defer conn.Close()
	authClient := authpb.NewAuthServiceClient(conn)

	// 2. Initialize Postgres Driver SQL Pool
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Fatal: Database initialization error: %v", err)
	}
	defer db.Close()

	// 3. Initialize Official Confluent Kafka Producer Instance
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
		"client.id":         "gateway_payment_producer",
		"acks":              "all",
	})
	if err != nil {
		log.Fatalf("Fatal: Kafka client exception: %v", err)
	}
	defer producer.Close()

	// Spawn delivery background routine tracking acks off Kafka event loops channel
	go listenToKafkaDeliveryReports(producer)

	// 4. Start Pipeline Channels & Goroutine Worker Pools
	jobQueue := make(chan TransactionJob, 500)
	for w := 1; w <= 5; w++ {
		go transactionWorker(w, jobQueue, db, producer)
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Fatal: Network address bind exception: %v", err)
	}

	// 5. Register gRPC server containing the Middleware Interceptor handler
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(AuthUnaryInterceptor(authClient)),
	)
	paymentpb.RegisterPaymentServiceServer(grpcServer, &paymentServer{jobQueue: jobQueue})

	log.Printf("Success: Payment Service Engine running on port: %s", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Critical: Failed to boot Payment gRPC: %v", err)
	}

	// ─── THE GRACEFUL SHUTDOWN ROUTINE ──────────────────────────────────
	// Start the gRPC server inside its own independent goroutine
	// go func() {
	// 	log.Printf("SUCCESS: Complete System Online. Monitoring port %s", port)
	// 	if err := grpcServer.Serve(lis); err != nil {
	// 		log.Printf("Server execution stopped: %v", err)
	// 	}
	// }()

	// Create a channel to listen for incoming Linux OS terminal signals
	// shutdownSignal := make(chan os.Signal, 1)
	// signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)

	// This blocks main execution here until you press Ctrl+C or Docker stops the container
	// <-shutdownSignal
	// log.Println("[SHUTDOWN] Intercepted stop signal. Beginning graceful termination...")

	// Stop accepting new gRPC requests instantly
	// grpcServer.GracefulStop()
	// log.Println("[SHUTDOWN] gRPC server stopped accepting new inbound connections.")

	// Safely close our internal worker channel queue pipeline
	// close(jobQueue)
	// log.Println("[SHUTDOWN] Worker job queue channel closed successfully.")

	// Allow Confluent Kafka to flush remaining messages out to the network wires
	// producer.Flush(3000) // Gives the native client up to 3 seconds to clear its memory buffer
	// log.Println("[SHUTDOWN] Confluent Kafka producer buffer flushed. Goodbye.")
}
