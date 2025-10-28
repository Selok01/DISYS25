package main

import (
	"context"
	"fmt"
	"io"
	random "math/rand"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "chit-chat/grpc/pb"
	"chit-chat/server/utils"
)

func createMessage(maxSize int) string {
	var ind int
	var message string
	
	abcd := "abcdefghijklmnopqrstovwxyz"
	addrAbcd := []rune(abcd)

	// max size of messages is set to 64 for shorter log messages
	for range maxSize / 2 {
		ind = random.Intn(len(addrAbcd))
		message += string(addrAbcd[ind])
	}

	return message
}

func main() {
	conn, err := grpc.NewClient("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		msg := fmt.Sprintf("Failed to connect: %v", err)
		utils.LogMessage(-1, -1, utils.CLIENT, utils.ERROR, msg)
	}
	defer conn.Close()

	client := pb.NewChatServiceClient(conn)

	clientID := int32(time.Now().Unix() % 1000)

	clientClock := make([]int32, 1000)

	vc := &pb.VectorClock{
		Clock: clientClock,
	}

	stream, err := client.Chat(context.Background())
	if err != nil {
		msg := fmt.Sprintf("Failed to connect: %v", err)
		utils.LogMessage(clientID, clientClock[clientID], utils.CLIENT, utils.ERROR, msg)
		return
	}

	// ----- Send Join -----
	joinReq := &pb.ClientRequest{
		Payload: &pb.ClientRequest_Join{
			Join: &pb.JoinRequest{
				ClientId: clientID,
				VecClock: vc,
			},
		},
	}

	utils.IncreaseClock(clientClock, clientID)
	msg := fmt.Sprintf("Participant %d joined chit-chat at logical time %v", clientID, clientClock[clientID])
	utils.LogMessage(clientID, clientClock[clientID], utils.CLIENT, utils.JOIN, msg)
	if err := stream.Send(joinReq); err != nil {
		utils.LogMessage(clientID, clientClock[clientID], utils.CLIENT, utils.ERROR, fmt.Sprintf("Failed to send join: %v", err))
		return
	}

	// ----- Receive messages in background -----
	go func() {
		for {
			reply, err := stream.Recv()
			if err == io.EOF {
				utils.LogMessage(clientID, clientClock[clientID], utils.CLIENT, utils.STREAM_END, "Server closed stream")
				return
			}
			if err != nil {
				utils.LogMessage(clientID, clientClock[clientID], utils.CLIENT, utils.ERROR, fmt.Sprintf("Stream receive error: %v", err))
				return
			}

			clientClock = utils.CompareClocks(reply.VecClock.Clock, clientClock)
			utils.IncreaseClock(clientClock, clientID)
			utils.LogMessage(clientID, clientClock[clientID], utils.CLIENT, utils.MESSAGE_RECEIVED, fmt.Sprintf("\"%s\"", reply.Ack))
		}
	}()

	// ----- Send message -----
	time.Sleep(2 * time.Second)
	clientMsg := createMessage(128)
	msgReq := &pb.ClientRequest{
		Payload: &pb.ClientRequest_Message{
			Message: &pb.SendMessage{
				ClientId: clientID,
				Message:  clientMsg,
				VecClock: vc,
			},
		},
	}
	utils.IncreaseClock(clientClock, clientID)
	if err := stream.Send(msgReq); err != nil {
		utils.LogMessage(clientID, clientClock[clientID], utils.CLIENT, utils.ERROR, fmt.Sprintf("Failed to send message: %v", err))
	}

	// ----- Sleep to simulate work -----
	time.Sleep(10 * time.Second)

	// ----- Send Leave -----
	leaveReq := &pb.ClientRequest{
		Payload: &pb.ClientRequest_Leave{
			Leave: &pb.LeaveRequest{
				ClientId: clientID,
				VecClock: vc,
			},
		},
	}
	
	utils.IncreaseClock(clientClock, clientID)
	if err := stream.Send(leaveReq); err != nil {
		utils.LogMessage(clientID, clientClock[clientID], utils.CLIENT, utils.ERROR, fmt.Sprintf("Failed to send leave: %v", err))
	}

	utils.LogMessage(clientID, clientClock[clientID], utils.CLIENT, utils.LEAVE, "Leaving chit-chat")
	time.Sleep(1 * time.Second)

	utils.LogMessage(clientID, clientClock[clientID], utils.CLIENT, utils.SHUTDOWN, "Client exited cleanly")
}
