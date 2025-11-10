package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "ra-ME/grpc/pb"
)

type node struct {
	pb.UnimplementedNodeServiceServer
	me, seq, highestSeq int32
	awaitingReplies, criticalTimes int
	reqCritical         bool
	replyDeferred       map[int32]bool
	mu                  sync.Mutex
	nodes_to_port       map[int32]pb.NodeServiceClient
}

func newNode(id int32) *node {
	return &node{
		me:            id,
		replyDeferred: make(map[int32]bool),
		nodes_to_port: make(map[int32]pb.NodeServiceClient),
	}
}

// funcitons to start nodes

func (n *node) startServer(port int) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterNodeServiceServer(grpcServer, n)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	grpcServer.GracefulStop()
	lis.Close()
	
}

func (n *node) connectToPeers(addresses []string) {
	for idx, addr := range addresses {
		peerID := idx + 1
		if peerID >= int(n.me) {
			peerID++
		}

		conn := n.dialWithRetry(addr)
		n.nodes_to_port[int32(peerID)] = pb.NewNodeServiceClient(conn) // storing the peers as "clients"
	}
}

func (n *node) dialWithRetry(addr string) *grpc.ClientConn {
	for range 3 {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			return conn
		}
		time.Sleep(1 * time.Second)
	}

	log.Fatalf("Failed to connect to %s", addr)
	return nil
}

//grpc sever handler

func (n *node) Node(ctx context.Context, req *pb.Request) (*pb.Reply, error) {
	if req.ForReplying {
		n.mu.Lock()
        n.awaitingReplies--
        n.mu.Unlock()
        log.Printf("[Node %d] Received deferred ACK from Node %d", n.me, req.NodeId)
        return &pb.Reply{Ack: true}, nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	requester := req.NodeId
	k := req.SequenceN

	// Track highest sequence number seen
	if k > n.highestSeq {
		n.highestSeq = k
	}

	// Decide if we should defer
	deferIt := n.shouldDefer(k, requester)

	if deferIt {
		n.replyDeferred[requester] = true
		log.Printf("[Node %d] Deferring reply to Node %d", n.me, requester)
		return &pb.Reply{Ack: false}, nil
	}

	log.Printf("[Node %d] Replying immediately to Node %d", n.me, requester)
	return &pb.Reply{Ack: true}, nil
}

// helper functions for the Ricard-Aggrawala algo

func (n *node) shouldDefer(k int32, j int32) bool {
	return n.reqCritical && ((k > n.seq) || (k == n.seq && j > n.me))
}

func (n *node) Request() {
	n.mu.Lock()
	n.reqCritical = true
	n.seq = n.highestSeq + 1
	n.awaitingReplies = len(n.nodes_to_port)
	req := &pb.Request{
		NodeId: n.me, 
		SequenceN: n.seq,
		ForReplying: false,
	}
	n.mu.Unlock()

	log.Printf("[Node %d] Sending request with seq=%d", n.me, n.seq)

	var wg sync.WaitGroup
	for id, client := range n.nodes_to_port {
		wg.Add(1)
		go func(id int32, client pb.NodeServiceClient) {
			defer wg.Done()
			reply, err := client.Node(context.Background(), req)
			if err != nil {
				log.Printf("[Node %d] Failed to contact Node %d: %v", n.me, id, err)
				return
			}
			if reply.Ack {
				n.mu.Lock()
				n.awaitingReplies--
				n.mu.Unlock()
				log.Printf("[Node %d] Received ACK from Node %d", n.me, id)
			}
		}(id, client)
	}
	wg.Wait()

	n.enterCriticalSection()
}

func (n *node) enterCriticalSection() {
	for n.awaitingReplies > 0 {
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("[Node %d] Entering critical section", n.me)
	n.mu.Lock()
	n.criticalTimes++
	n.mu.Unlock()
	time.Sleep(2 * time.Second)

	n.exitCriticalSection()
}

func (n *node) exitCriticalSection() {
	n.mu.Lock()
	n.reqCritical = false

	log.Printf("[Node %d] Leaving critical section", n.me)

	// Send replies to all deferred nodes
	for j, deferred := range n.replyDeferred {
		if deferred {
			log.Printf("[Node %d] Sending deferred reply to Node %d", n.me, j)
			n.replyDeferred[j] = false
			go n.Reply(j)
		}
	}
	n.mu.Unlock()
}

func (n *node) Reply(target int32) {
	client, ok := n.nodes_to_port[target]
	if !ok {
		log.Printf("[Node %d] No client for Node %d", n.me, target)
		return
	}

	_, err := client.Node(context.Background(), &pb.Request{
		NodeId:    n.me,
		SequenceN: n.highestSeq,
		ForReplying: true,
	})
	if err != nil {
		log.Printf("[Node %d] Failed to send reply to Node %d: %v", n.me, target, err)
	}
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Please provide arguments in this order: go run main.go <id> <port> <peers>")
		os.Exit(1)
	}

	// Receive and process cli arguments
	nodeId, _ := strconv.Atoi(os.Args[1])
	port, _ := strconv.Atoi(os.Args[2])
	peerAddresses := strings.Split(strings.TrimSuffix(os.Args[3], ","), ",")

	// Create and start node
	n := newNode(int32(nodeId))
	go n.startServer(port)

	// Wait for all servers to be ready
	time.Sleep(2 * time.Second)

	// Connect to all peers
	n.connectToPeers(peerAddresses)

	go func() {
		for {
			n.mu.Lock()
			if n.criticalTimes >= 1 {
				n.mu.Unlock()
				break
			}
			n.mu.Unlock()

			time.Sleep(time.Duration(int32(rand.Intn(5)) + n.me) * time.Second)
			n.Request()
		}
	}()

	select {}
}
