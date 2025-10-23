package main

import (
	"context"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"

	pb "Assign3/pb"
)

func main() {

	conn, err := grpc.Dial("localhost:8080", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("[Client] [ERROR] Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewServerServiceClient(conn)

	clientID := int32(time.Now().Unix() % 1000)

	joinReq := &pb.JoinRequest{
		ClientId: clientID,
	}

	stream, err := client.Join(context.Background(), joinReq)
	if err != nil {
		log.Fatalf("[Client] [ERROR] Join failed: %v", err)
	}

	log.Printf("[Client] [JOIN] Joined Chit Chat as ID=%d", clientID)

	go func() {
		for {
			reply, err := stream.Recv()
			if err == io.EOF {
				log.Printf("[Client] [STREAM_END] Server closed stream for ClientID=%d", clientID)
				return
			}
			if err != nil {
				log.Printf("[Client] [ERROR] Stream receive error: %v", err)
				return
			}
			log.Printf("[Client] [RECEIVED] Message=\"%s\"", reply.Ack)
		}
	}()

	time.Sleep(5 * time.Second)

	leaveReq := &pb.LeaveRequest{
		ClientId: clientID,
	}

	leaveStream, err := client.Leave(context.Background(), leaveReq)
	if err != nil {
		log.Fatalf("[Client] [ERROR] Leave failed: %v", err)
	}
	defer leaveStream.CloseSend()

	log.Printf("[Client] [LEAVE] Leaving Chit Chat, ClientID=%d", clientID)
	time.Sleep(1 * time.Second)

	log.Println("[Client] [SHUTDOWN] Client exiting cleanly")
}
