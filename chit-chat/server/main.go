package main

import (
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"google.golang.org/grpc"

	pb "chit-chat/grpc/pb"
	"chit-chat/server/utils"
)

type chatService struct {
	pb.UnimplementedChatServiceServer
	mu      sync.Mutex
	clients map[int32]pb.ChatService_ChatServer
	clock   []int32
}

func NewServer() *chatService {
	return &chatService{
		clients: make(map[int32]pb.ChatService_ChatServer),
		clock:   make([]int32, 1000),
	}
}

func (s *chatService) Chat(stream pb.ChatService_ChatServer) error {
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

func (s *chatService) Join(req *pb.JoinRequest, stream pb.ChatService_ChatServer) error {
	clientID := req.ClientId
	clientClock := req.VecClock.Clock

	s.mu.Lock()
	s.clients[clientID] = stream
	s.clock = utils.CompareClocks(s.clock, clientClock)
	s.mu.Unlock()

	utils.IncreaseClock(s.clock, 0)
	msg := fmt.Sprintf("Participant %d joined chit-chat at logical time %v", clientID, s.clock[0])
	utils.LogMessage(-1, s.clock[0], utils.SERVER, utils.JOIN, msg)

	vc := &pb.VectorClock{
		Clock: s.clock,
	}

	s.broadcast(&pb.ServerReply{
		Ack:      msg,
		VecClock: vc,
	}, 0)

	return nil
}

func (s *chatService) Leave(req *pb.LeaveRequest, stream pb.ChatService_ChatServer) error {
	clientID := req.ClientId
	clientClock := req.VecClock.Clock

	s.mu.Lock()
	s.clients[clientID] = stream
	s.clock = utils.CompareClocks(s.clock, clientClock)
	s.mu.Unlock()

	utils.IncreaseClock(s.clock, 0)
	msg := fmt.Sprintf("Participant %d left Chit Chat at logical time %v", clientID, s.clock[0])
	utils.LogMessage(-1, s.clock[0], utils.SERVER, utils.LEAVE, msg)

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

func (s *chatService) Message(req *pb.SendMessage, stream pb.ChatService_ChatServer) error {
	clientID := req.ClientId
	msg := req.Message
	clientClock := req.VecClock.Clock

	s.mu.Lock()
	s.clients[clientID] = stream
	s.clock = utils.CompareClocks(s.clock, clientClock)
	s.mu.Unlock()

	utils.IncreaseClock(s.clock, 0)
	clientMessage := fmt.Sprintf("CLIENT-%d messaged: \"%s\"", clientID, msg)
	utils.LogMessage(-1, s.clock[0], utils.SERVER, utils.MESSAGE_SEND, clientMessage)

	vc := &pb.VectorClock{
		Clock: s.clock,
	}

	s.broadcast(&pb.ServerReply{
		Ack:      clientMessage,
		VecClock: vc,
	}, 0)

	return nil
}

func (s *chatService) broadcast(reply *pb.ServerReply, excludeID int32) {
	// Copy clients under lock to avoid holding the mutex while sending
	s.mu.Lock()
	clientsCopy := make(map[int32]pb.ChatService_ChatServer, len(s.clients))
	maps.Copy(clientsCopy, s.clients)
	s.mu.Unlock()

	for id, clientStream := range clientsCopy {
		if excludeID != 0 && id == excludeID {
			continue
		}

		utils.IncreaseClock(s.clock, 0)
		if err := clientStream.Send(reply); err != nil {
			msg := fmt.Sprintf("Broadcast failed for CLIENT-%d: %v", id, err)
			utils.LogMessage(-1, s.clock[0], utils.SERVER, utils.ERROR, msg)

			// Remove the broken client safely
			s.mu.Lock()
			delete(s.clients, id)
			s.mu.Unlock()

			continue
		}

		msg := fmt.Sprintf("Broadcast delivered to CLIENT-%d: \"%s\"", id, reply.Ack)
		utils.LogMessage(-1, s.clock[0], utils.SERVER, utils.BROADCAST, msg)
	}

	msg := fmt.Sprintf("\"%s\"", reply.Ack)
	utils.LogMessage(-1, s.clock[0], utils.SERVER, utils.BROADCAST, msg)
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

		utils.IncreaseClock(server.clock, 0)
		utils.LogMessage(-1, server.clock[0], utils.SERVER, utils.SHUTDOWN, "Server shutting down gracefully...")
		grpcServer.GracefulStop()
		lis.Close()
		utils.LogMessage(-1, server.clock[0], utils.SERVER, utils.SHUTDOWN, "Completed")
	}()

	if err := grpcServer.Serve(lis); err != nil {
		msg := fmt.Sprintf("Failed to serve: %v", err)
		utils.LogMessage(-1, server.clock[0], utils.SERVER, utils.ERROR, msg)
	}
}
