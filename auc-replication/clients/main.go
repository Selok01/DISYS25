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

func Bid(amount int32, id int32) *pb.ClientRequest {

	return &pb.ClientRequest{
		Payload: &pb.ClientRequest_Bid{
			Bid: &pb.BidRequest{
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

func main() {
	conn, err := grpc.NewClient("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		msg := fmt.Sprintf("Failed to connect: %v", err)
		log.Fatal(msg)
	}
	defer conn.Close()

	client := pb.NewAuctionServiceClient(conn)
	clientID := int32(time.Now().Unix() % 1000)

	for range 30 {
		// ----- Send Bid -----
		bid := Bid(int32(random.Intn(10000)), clientID)
		reply, _ := client.Auction(context.Background(), bid)
		log.Print(reply)

		time.Sleep(1 * time.Second)

		// ----- Get current state -----
		result := Result(clientID)

		reply, _ = client.Auction(context.Background(), result)
		log.Print(reply)

		time.Sleep(1 * time.Second)
	}

}
