package main

import (
	"fmt"
	"io"
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
	pb.UnimplementedChatServiceServer

	mu      sync.Mutex
	clients map[int32]pb.ChatService_ChatServer
	clock   []int32
}

func NewServer() *serverService {
	return &serverService{
		clients: make(map[int32]pb.ChatService_ChatServer),
		clock:   make([]int32, 1000),
	}
}

func (s *serverService) Inc_clock() {
	s.clock[0]++
}

func compare_clock(vec1, vec2 []int32) []int32 {

	if len(vec1) != len(vec2) {
		panic("cant compare vector clocks of different length")
	}

	merged := make([]int32, 1000)

	for i := range len(vec1) {
		if vec1[i] > vec2[i] {
			merged[i] = vec1[i]
		} else {
			merged[i] = vec2[i]
		}
	}

	return merged
}

func (s *serverService) Chat(stream pb.ChatService_ChatServer) error {
	var clientID int32

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if clientID != 0 {
				s.mu.Lock()
				delete(s.clients, clientID)
				s.mu.Unlock()
			}
			return err
		}

		switch payload := req.Payload.(type) {
		case *pb.ClientRequest_Join:
			clientID = payload.Join.ClientId
			s.Join(payload.Join, stream)

		case *pb.ClientRequest_Message:
			s.Message(payload.Message, stream)

		case *pb.ClientRequest_Leave:
			s.Leave(payload.Leave, stream)
			return nil
		}
	}

	if clientID != 0 {
		s.mu.Lock()
		delete(s.clients, clientID)
		s.mu.Unlock()
	}

	return nil
}

func (s *serverService) Join(req *pb.JoinRequest, stream pb.ChatService_ChatServer) error {
	clientID := req.ClientId
	clientClock := req.VecClock.Clock

	s.mu.Lock()
	s.clients[clientID] = stream
	s.clock = compare_clock(s.clock, clientClock)
	s.mu.Unlock()

	s.Inc_clock()
	msg := fmt.Sprintf("CLIENT-%d joined chit-chat", clientID)
	utils.LogMessage(-1, int(s.clock[0]), utils.SERVER, utils.JOIN, msg)

	vc := &pb.VectorClock{
		Clock: s.clock,
	}

	s.broadcast(&pb.ServerReply{
		Ack:      msg,
		VecClock: vc,
	}, 0)

	return nil
}

func (s *serverService) Leave(req *pb.LeaveRequest, stream pb.ChatService_ChatServer) error {
	clientID := req.ClientId
	clientClock := req.VecClock.Clock

	s.mu.Lock()
	s.clients[clientID] = stream
	s.clock = compare_clock(s.clock, clientClock)
	s.mu.Unlock()

	s.Inc_clock()
	msg := fmt.Sprintf("CLIENT-%d left Chit Chat", clientID)
	utils.LogMessage(-1, int(s.clock[0]), utils.SERVER, utils.LEAVE, msg)

	vc := &pb.VectorClock{
		Clock: s.clock,
	}

	s.broadcast(&pb.ServerReply{
		Ack:      msg,
		VecClock: vc,
	}, 0)

	<-stream.Context().Done()

	s.mu.Lock()
	delete(s.clients, clientID)
	s.mu.Unlock()

	return nil
}

func (s *serverService) Message(req *pb.SendMessage, stream pb.ChatService_ChatServer) error {
	clientID := req.ClientId
	msg := req.Message
	clientClock := req.VecClock.Clock

	s.mu.Lock()
	s.clients[clientID] = stream
	s.clock = compare_clock(s.clock, clientClock)
	s.mu.Unlock()

	s.Inc_clock()
	clientMessage := fmt.Sprintf("CLIENT-%d messaged: \"%s\"", clientID, msg)
	utils.LogMessage(-1, int(s.clock[0]), utils.SERVER, utils.MESSAGE_SEND, clientMessage)

	vc := &pb.VectorClock{
		Clock: s.clock,
	}

	s.broadcast(&pb.ServerReply{
		Ack:      clientMessage,
		VecClock: vc,
	}, 0)

	return nil
}

func (s *serverService) broadcast(reply *pb.ServerReply, excludeID int32) {
	// Copy clients under lock to avoid holding the mutex while sending
	s.mu.Lock()
	clientsCopy := make(map[int32]pb.ChatService_ChatServer, len(s.clients))
	for id, stream := range s.clients {
		clientsCopy[id] = stream
	}
	s.mu.Unlock()

	for id, clientStream := range clientsCopy {
		if excludeID != 0 && id == excludeID {
			continue
		}

		if err := clientStream.Send(reply); err != nil {
			msg := fmt.Sprintf("Broadcast failed for CLIENT-%d: %v", id, err)
			utils.LogMessage(-1, int(reply.VecClock.Clock[0]), utils.SERVER, utils.ERROR, msg)

			// Remove the broken client safely
			s.mu.Lock()
			delete(s.clients, id)
			s.mu.Unlock()

			continue
		}

		msg := fmt.Sprintf("Broadcast delivered to CLIENT-%d: \"%s\"", id, reply.Ack)
		utils.LogMessage(-1, int(reply.VecClock.Clock[0]), utils.SERVER, utils.BROADCAST, msg)
	}

	msg := fmt.Sprintf("\"%s\"", reply.Ack)
	utils.LogMessage(-1, int(reply.VecClock.Clock[0]), utils.SERVER, utils.BROADCAST, msg)
}

func main() {
	log.SetFlags(0)
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen on port 8080: %v", err)
	}

	grpcServer := grpc.NewServer()
	server := NewServer()
	pb.RegisterChatServiceServer(grpcServer, server)

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
