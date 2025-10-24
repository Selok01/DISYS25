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

	pb "chit-chat/grpc/pb"
	"chit-chat/server/utils"
)

type serverService struct {
	pb.UnimplementedSServiceServer
	mu      sync.Mutex
	clients map[int32]pb.SService_JoinServer
	clock   map[int32]int32
}

func NewServer() *serverService {
	return &serverService{
		clients: make(map[int32]pb.SService_JoinServer),
		clock:   make(map[int32]int32),
	}
}

func (s *serverService) Inc_clock() {
	s.clock[0]++
}

func (s *serverService) Join(req *pb.JoinRequest, stream pb.SService_JoinServer) error {

	clientID := req.ClientId

	s.mu.Lock()
	s.clients[clientID] = stream
	s.clock[clientID] = 0
	s.Inc_clock()
	s.mu.Unlock()

	msg := fmt.Sprintf("CLIENT-%d joined chit-chat", clientID)
	utils.LogMessage(-1, -1, utils.SERVER, utils.JOIN, msg)

	s.broadcast(&pb.ServerReply{Ack: msg}, 0)

	<-stream.Context().Done()
	return nil
}

func (s *serverService) Leave(req *pb.LeaveRequest, stream pb.SService_LeaveServer) error {
	clientID := req.ClientId

	s.mu.Lock()
	s.clients[clientID] = stream
	s.mu.Unlock()

	joinMsg := fmt.Sprintf("CLIENT-%d left Chit Chat", clientID)
	utils.LogMessage(-1, -1, utils.SERVER, utils.LEAVE, joinMsg)

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

func (s *serverService) Message(req *pb.SendMessage, stream pb.SService_MessageServer) error {
	clientID := req.ClientId
	msg := req.Message

	s.mu.Lock()
	s.clients[clientID] = stream
	s.mu.Unlock()

	
	clientMessage := fmt.Sprintf("CLIENT-%d messaged: \"%s\"", clientID, msg)
	utils.LogMessage(-1, -1, utils.SERVER, utils.LEAVE, clientMessage)

	s.broadcast(&pb.ServerReply{
		Ack:      clientMessage,
		VecClock: req.VecClock,
	}, 0)

	<-stream.Context().Done()

	s.mu.Lock()
	delete(s.clients, clientID)
	s.mu.Unlock()

	return nil
}


func (s *serverService) broadcast(reply *pb.ServerReply, excludeID int32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, clientStream := range s.clients {
		if excludeID != 0 && id == excludeID {
			continue
		}

		err := clientStream.SendMsg(reply)
		if err != nil {
			msg := fmt.Sprintf("Broadcast failed for CLIENT-%d: %v", id, err)
			utils.LogMessage(-1, -1, utils.SERVER, utils.ERROR, msg)
			delete(s.clients, id)
			continue
		}

		msg := fmt.Sprintf("Broadcast delivered to CLIENT-%d: \"%s\"", id, reply.Ack)
		utils.LogMessage(-1, -1, utils.SERVER, utils.BROADCAST, msg)
	}

	msg := fmt.Sprintf("\"%s\"", reply.Ack)
	utils.LogMessage(-1, -1, utils.SERVER, utils.BROADCAST, msg)
}

func main() {
	log.SetFlags(0)
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen on port 8080: %v", err)
	}

	grpcServer := grpc.NewServer()
	server := NewServer()
	pb.RegisterSServiceServer(grpcServer, server)

	log.Println("Server started, now listening on :8080")

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan

		utils.LogMessage(-1, -1, utils.SERVER, utils.SHUTDOWN, "Server shutting down gracefully...")
		grpcServer.GracefulStop()
		lis.Close()
		utils.LogMessage(-1, -1, utils.SERVER, utils.SHUTDOWN, "Completed")
		os.Exit(0)
	}()

	if err := grpcServer.Serve(lis); err != nil {
		msg := fmt.Sprintf("Failed to serve: %v", err)
		utils.LogMessage(-1, -1, utils.SERVER, utils.ERROR, msg)
	}
}
