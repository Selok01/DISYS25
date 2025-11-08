package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc"

	pb "ra-ME/grpc/pb"
)

type node struct {
	pb.UnimplementedNodeServiceServer
	me, seq, highestSeq int32
	awaitingReplies int 
	reqCritical, replyDeferred bool
	mu sync.Mutex
	nodes_to_port map[int32]pb.NodeServiceClient
}

func newNode(id int32) *node {
	return &node{
		me: id,
		nodes_to_port: make(map[int32]pb.NodeServiceClient),
	}
}
/*

func (node *nodeService) request(req *pb.Request, stream pb.NodeService_NodeClient) error {

	return 
}

func (node *nodeService) reply(req *pb.Reply, stream pb.NodeService_NodeClient) error {

}
*/


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

	log.Printf("Node %d ready, dialing to ports: %v", nodeId, n.nodes_to_port)

}