package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	random "math/rand"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "auc-replication/grpc/pb"
)

func Bid(amount int32, id int32) *pb.ClientRequest {
	return &pb.ClientRequest{
		Payload: &pb.ClientRequest_Bid{
			Bid: &pb.Bidder{
				ClientId: id,
				Amount:   amount,
			},
		},
	}
}

func Result(id int32) *pb.ClientRequest {
	return &pb.ClientRequest{
		Payload: &pb.ClientRequest_Result{
			Result: &pb.ResultRequest{
				ClientId: id,
			},
		},
	}
}

func getClient() (pb.AuctionServiceClient, *grpc.ClientConn, error) {
    ports := []int{50001, 50002}
    
    for _, port := range ports {
        conn, err := grpc.NewClient(
            fmt.Sprintf("localhost:%d", port),
            grpc.WithTransportCredentials(insecure.NewCredentials()),
        )
        if err != nil {
            continue
        }
        
        // Test if server responds
        client := pb.NewAuctionServiceClient(conn)
        ctx, cancel := context.WithTimeout(context.Background(), time.Second)
        _, err = client.Auction(ctx, Result(0))
        cancel()
        
        if err == nil {
            return client, conn, nil
        }
        conn.Close()
    }
    
    return nil, nil, fmt.Errorf("no server available")
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Please provide arguments in this order: go run main.go <clientID>")
		os.Exit(1)
	}

	clientID, _ := strconv.Atoi(os.Args[1])

	// Infinite loop to use proper server connection before sending a request (allows reconnection to new Leader nodes)
	for {
        // ----- Send Bid -----
        client, conn, err := getClient()
        if err != nil {
            log.Printf("No server available, retrying in 1s...")
            time.Sleep(1 * time.Second)
            continue
        }
        
        bid := Bid(int32(random.Intn(10000)), int32(clientID))
        reply, err := client.Auction(context.Background(), bid)
        if err != nil {
            log.Printf("Bid failed: %v, retrying...", err)
            conn.Close()
            time.Sleep(1 * time.Second)
            continue
        }
        log.Print(reply)
        conn.Close()
        time.Sleep(1 * time.Second)
        
        // ----- Send Result (to get current auction state) -----
        client, conn, err = getClient()
        if err != nil {
            log.Printf("No server available, retrying in 1s...")
            time.Sleep(2 * time.Second)
            continue
        }
        
        result := Result(int32(clientID))
        reply, err = client.Auction(context.Background(), result)
        if err != nil {
            log.Printf("Result failed: %v", err)
        } else {
            log.Print(reply)
        }
        conn.Close()
        time.Sleep(1 * time.Second)
    }
}