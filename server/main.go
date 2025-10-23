package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"google.golang.org/grpc"

	pb "Assign3/pb"
)

type ServerServiceServerImpl struct {
	pb.UnimplementedServerServiceServer
	mu      sync.Mutex
	clients map[int32]pb.ServerService_JoinServer
	clock   map[int32]int32
}

func NewServer() *ServerServiceServerImpl {
	return &ServerServiceServerImpl{
		clients: make(map[int32]pb.ServerService_JoinServer),
		clock:   make(map[int32]int32),
	}
}

func (s *ServerServiceServerImpl) Inc_clock() {
	s.clock[0]++
}

func (s *ServerServiceServerImpl) Join(req *pb.JoinRequest, stream pb.ServerService_JoinServer) error {

	clientID := req.ClientId

	s.mu.Lock()
	s.clients[clientID] = stream
	s.clock[clientID] = 0
	s.Inc_clock()
	s.mu.Unlock()

	msg := fmt.Sprintf("Participant %d joined Chit Chat, clientID", clientID)

	log.Printf("[Server] [JOIN]: ClientID=%d Message=\"%s\"", clientID, msg)

	s.broadcast(&pb.ServerReply{Ack: msg}, 0)

	<-stream.Context().Done()
	return nil
}

func (s *ServerServiceServerImpl) Leave(req *pb.LeaveRequest, stream pb.ServerService_LeaveServer) error {
	clientID := req.ClientId

	s.mu.Lock()
	s.clients[clientID] = stream
	s.mu.Unlock()

	joinMsg := fmt.Sprintf("Participant %d left Chit Chat, clientID", clientID)
	log.Printf("[Server] [Leave]: ClientID=%d Message=\"%s\"", clientID, joinMsg)

	s.broadcast(&pb.ServerReply{
		Ack:      joinMsg,
		VecClock: req.VecClock,
	}, 0)

	<-stream.Context().Done()

	s.mu.Lock()
	delete(s.clients, clientID)
	s.mu.Unlock()

	return nil
}

func (s *ServerServiceServerImpl) broadcast(reply *pb.ServerReply, excludeID int32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, clientStream := range s.clients {
		if excludeID != 0 && id == excludeID {
			continue
		}

		err := clientStream.SendMsg(reply)
		if err != nil {
			log.Printf("[Server] [ERROR]: Event=BROADCAST_FAIL ClientID=%d Error=%v", id, err)
			delete(s.clients, id)
			continue
		}

		log.Printf("[Server] [BROADCAST_DELIVERED]: ToClientID=%d Message=\"%s\"", id, reply.Ack)
	}

	log.Printf("[Server] [BROADCAST]: Message=\"%s\"", reply.Ack)
}

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("failed to listen on port 8080: %v", err)
	}

	grpcServer := grpc.NewServer()
	server := NewServer()
	pb.RegisterServerServiceServer(grpcServer, server)

	log.Println("Server listening on :8080")

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan

		log.Println("[Server] [SHUTDOWN]: Server shutting down gracefully...")
		grpcServer.GracefulStop()
		lis.Close()
		log.Println("[Server] [SHUTDOWN]: Completed")
		os.Exit(0)
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
