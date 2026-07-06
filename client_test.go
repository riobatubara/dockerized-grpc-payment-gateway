package main

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	authpb "dockerized-grpc-payment-gateway/proto/auth"
	paymentpb "dockerized-grpc-payment-gateway/proto/payment"
)

func TestPaymentGatewayPipeline(t *testing.T) {
	// 1. Establish connection to Auth Service (Port 50051)
	authConn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to auth service: %v", err)
	}
	defer authConn.Close()
	authClient := authpb.NewAuthServiceClient(authConn)

	// 2. Establish connection to Payment Service (Port 50052)
	paymentConn, err := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to payment service: %v", err)
	}
	defer paymentConn.Close()
	paymentClient := paymentpb.NewPaymentServiceClient(paymentConn)

	// 3. Login dynamically to retrieve an active Redis session token
	ctx := context.Background()
	loginRes, err := authClient.Login(ctx, &authpb.LoginRequest{
		Username: "admin",
		Password: "password123",
	})
	if err != nil {
		t.Fatalf("Authentication Login failed: %v", err)
	}
	sessionToken := loginRes.Token
	log.Printf("[CLIENT] Successfully logged in! Received Session Token: %s", sessionToken)

	// 4. Setup Concurrency Stress Testing using WaitGroups
	var wg sync.WaitGroup
	concurrentShoppers := 10 // Simulating 10 checkouts hitting the pipeline at the exact same instant

	log.Printf("[CLIENT] Starting stress test with %d concurrent transaction jobs...", concurrentShoppers)

	for i := 1; i <= concurrentShoppers; i++ {
		wg.Add(1)

		// Spawn an independent shopper routine
		go func(shopperID int) {
			defer wg.Done()

			// Create a 5-second Context window for this specific user's web request
			reqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Inject the Redis token into the gRPC outward transport metadata (Like HTTP Authorization Headers)
			md := metadata.Pairs("authorization", sessionToken)
			outboundCtx := metadata.NewOutgoingContext(reqCtx, md)

			// Submit the transaction request payload
			res, err := paymentClient.ProcessPayment(outboundCtx, &paymentpb.PaymentRequest{
				Amount:             99.99 * float64(shopperID),
				Currency:           "USD",
				DestinationAccount: "acct_target_marketplace",
			})

			if err != nil {
				log.Printf("[SHOPPER %d] ❌ Payment Failed: %v", shopperID, err)
				return
			}

			log.Printf("[SHOPPER %d]  Payment Succeeded! ID: %s | Status: %s", shopperID, res.TransactionId, res.Status)
		}(i)
	}

	// Block and wait for all concurrent worker actions to clear
	wg.Wait()
	log.Printf("[CLIENT] Concurrency testing loop completed successfully.")
}
