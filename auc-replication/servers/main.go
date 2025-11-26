package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "auc-replication/grpc/pb"
)

type auctionService struct {
	pb.UnimplementedAuctionServiceServer
	mu 								 sync.Mutex
	bidders                          map[int32]int32
	highestBid, highestBidder, round int32
	over                             bool
	leader 							 bool										
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
		if !a.leader {
            conn, err := grpc.NewClient("localhost:50001", grpc.WithTransportCredentials(insecure.NewCredentials()))
            if err != nil {
                return nil, fmt.Errorf("failed to connect to leader: %v", err)
            }
            defer conn.Close()
            leaderClient := pb.NewAuctionServiceClient(conn)

            // Redirect the bid request to the leader node
            return leaderClient.Auction(context.Background(), req)
		}
		
		res = a.Bid(payload.Bid)

	case *pb.ClientRequest_Result:
		res = a.Result()
	}

	return &pb.ServerReply{Ack: res}, nil
}

func (a *auctionService) Bid(req *pb.Bidder) string {
	a.mu.Lock()
    defer a.mu.Unlock()

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

	// Replicate to follower node
    go a.replicateState()

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

func (a *auctionService) Replicate(ctx context.Context, msg *pb.ReplicationMessage) (*pb.Ack, error) {
    a.mu.Lock()
    defer a.mu.Unlock()
    
    a.highestBid = msg.HighestBid
    a.highestBidder = msg.HighestBidder
    a.round = msg.Round
    a.over = msg.Over
    
    // Reconstruct bidders map
    a.bidders = make(map[int32]int32)
    for _, bidder := range msg.Bidders {
        a.bidders[bidder.ClientId] = bidder.Amount
    }
    
    return &pb.Ack{Success: true}, nil
}

func (a *auctionService) replicateState() {
    conn, err := grpc.NewClient("localhost:50002", grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return
    }
    defer conn.Close()
    
    client := pb.NewAuctionServiceClient(conn)
    a.mu.Lock()
    biddersList := make([]*pb.Bidder, 0, len(a.bidders))
    for clientId, amount := range a.bidders {
        biddersList = append(biddersList, &pb.Bidder{
            ClientId: clientId,
            Amount:   amount,
        })
    }
    
    msg := &pb.ReplicationMessage{
        HighestBid:    a.highestBid,
        HighestBidder: a.highestBidder,
        Round:         a.round,
        Over:          a.over,
        Bidders:       biddersList,
    }
    a.mu.Unlock()
    
    client.Replicate(context.Background(), msg)
}

func (a *auctionService) Heartbeat(ctx context.Context, ping *pb.Ping) (*pb.Pong, error) {
    return &pb.Pong{}, nil
}

func (a *auctionService) startHeartbeat() {
	if a.leader {
		return
	}

    go func() {
		for {
			conn, err := grpc.NewClient("localhost:50001", grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}
			
			client := pb.NewAuctionServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err = client.Heartbeat(ctx, &pb.Ping{})
			cancel()
			conn.Close()

			if err != nil {
				a.leader = true
				log.Println("Follower replica now became Leader (detected initial Leader failure)")
				return
            }

			time.Sleep(2 * time.Second)
        }
    }()
}

func (a *auctionService) reset() {
	a.mu.Lock()
    defer a.mu.Unlock()

	a.bidders = make(map[int32]int32)
	a.highestBid = 0
	a.highestBidder = 0
	a.over = false
	a.round++

	if a.leader {
        go a.replicateState()
    }
}

func (a *auctionService) endAuction() {
	a.mu.Lock()
    a.over = true
	a.mu.Unlock()

	if a.highestBid  != 0 {
		log.Printf("Auction %d ended. Winner: Participant %d with a bid of %d", a.round, a.highestBidder, a.highestBid)
	} else {
			log.Printf("Auction %d ended. No participants bid at all", a.round)
	}

	if a.leader {
        go a.replicateState()
    }
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Please provide arguments in this order: go run main.go <port>")
		os.Exit(1)
	}

	port, _ := strconv.Atoi(os.Args[1])
	lis, err := net.Listen("tcp", fmt.Sprint(":", port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", port, err)
	}

	grpcServer := grpc.NewServer()
	server := NewServer()
	pb.RegisterAuctionServiceServer(grpcServer, server)

	if port == 50001 {
		server.leader = true
	}

	server.startHeartbeat()

	log.Printf("Server started, now listening on :%d", port)

	go func() {
		for {
			server.reset()
			time.Sleep(15 * time.Second)

			if server.leader {
				server.endAuction()
			}
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
