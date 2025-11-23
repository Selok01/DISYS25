package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"google.golang.org/grpc"

	pb "auc-replication/grpc/pb"
)

type auctionService struct {
	pb.UnimplementedAuctionServiceServer
	mu      sync.Mutex
	bidders map[int32]int32
	highestBid, highestBidder int32
	over bool
}

func NewServer() *auctionService {
	return &auctionService {
		bidders: make(map[int32]int32),
	}
}

func (a *auctionService) Auction(ctx context.Context, req *pb.ClientRequest) (*pb.ServerReply, error) {
	var res string

	switch payload := req.Payload.(type) {
		case *pb.ClientRequest_Bid:
			res = a.Bid(payload.Bid)

		case *pb.ClientRequest_Result:
			res = a.Result()
	}

	return &pb.ServerReply{Ack: res}, nil
}

func (a *auctionService) Bid(req *pb.BidRequest) string {
	currentBid := req.Amount
	p := req.ClientId

	if a.highestBid < currentBid {
		a.highestBid = currentBid
		a.highestBidder = p
	}

	if _, exists := a.bidders[p]; !exists {
		a.bidders[p] = currentBid
	} 

	if a.bidders[p] < currentBid {
		a.bidders[p] = currentBid
		return "success"
	}

	return "fail"
}

func (a *auctionService) Result() string {
	var result string 

	if a.over {
		result = fmt.Sprintf("Participant %d won the auction with a bet of %d", a.highestBidder, a.highestBid)
		return result
	}


	result = fmt.Sprintf("Auction is not over yet, current highest bid is: %d", a.highestBid)
	return  result
}


/*

func broadcast2Servers() {

}

func broadcast2Clients(address string)  {



}

*/

func main() {
	log.SetFlags(0)
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen on port 8080: %v", err)
	}

	//broadcast2Clients(lis.Addr())

	grpcServer := grpc.NewServer()
	server := NewServer()
	pb.RegisterAuctionServiceServer(grpcServer, server)

	
	log.Println("Server started, now listening on :8080")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGINT)

	<-sigChan
	grpcServer.GracefulStop()
	lis.Close()
	log.Println("Server closed")
}
