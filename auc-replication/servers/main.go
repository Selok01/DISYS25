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
	"time"

	"google.golang.org/grpc"

	pb "auc-replication/grpc/pb"
)

type auctionService struct {
	pb.UnimplementedAuctionServiceServer
	mu                               sync.Mutex
	bidders                          map[int32]int32
	highestBid, highestBidder, round int32
	over                             bool
}

func NewServer() *auctionService {
	return &auctionService{
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

	if a.over {
		return "fail: auction is over"
	}

	if currentBid <= a.highestBid {
		return "fail"
	}

	// Update highest bid + highest bidder
	a.highestBid = currentBid
	a.highestBidder = p

	// Record bidder's best bid
	a.bidders[p] = currentBid

	return "success"
}

func (a *auctionService) Result() string {
	var result string

	if a.over {
		result = fmt.Sprintf("Participant %d won the auction with a bet of %d", a.highestBidder, a.highestBid)
		return result
	}

	result = fmt.Sprintf("Auction is not over yet, current highest bid is: %d", a.highestBid)
	return result
}

func (a *auctionService) reset() {

	a.bidders = make(map[int32]int32)
	a.highestBid = 0
	a.highestBidder = 0
	a.over = false
	a.round++
}

func (a *auctionService) closeAuc() {
	a.over = true
	log.Printf("Auction %d ended. Winner: %d with %d", a.round, a.highestBidder, a.highestBid)
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

	grpcServer := grpc.NewServer()
	server := NewServer()
	pb.RegisterAuctionServiceServer(grpcServer, server)

	log.Println("Server started, now listening on :8080")

	go func() {
		for {
			server.reset()
			time.Sleep(15 * time.Second)
			server.closeAuc()
		}
	}()

	go func() {

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGINT)

		<-sigChan
		grpcServer.GracefulStop()
		lis.Close()
		log.Println("Server closed")

	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
