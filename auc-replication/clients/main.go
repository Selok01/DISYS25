package main

import (
	"context"
	"fmt"
	"log"
	"time"

	random "math/rand"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "auc-replication/grpc/pb"
)

func main() {
	conn, err := grpc.NewClient("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		msg := fmt.Sprintf("Failed to connect: %v", err)
		log.Fatal(msg)
	}
	defer conn.Close()

	client := pb.NewAuctionServiceClient(conn)
	clientID := int32(time.Now().Unix() % 1000)


	// ----- Send Bid -----
	bid := &pb.ClientRequest{
		Payload: &pb.ClientRequest_Bid{
			Bid: &pb.BidRequest{
				ClientId: clientID,
				Amount: int32(random.Intn(100)),
			},
		},
	}

	_, error := client.Auction(context.Background(), bid)
	log.Print(error)

	// ----- Get current state -----
	result := &pb.ClientRequest{
		Payload: &pb.ClientRequest_Result{
			Result: &pb.ResultRequest{
				ClientId: clientID,
			},
		},
	}

	_, error = client.Auction(context.Background(), result)
	log.Print(error)

	select {}
}
