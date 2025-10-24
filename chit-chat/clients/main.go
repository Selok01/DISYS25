package main

import (
	"context"
	"fmt"
	"io"
	"time"
	random "math/rand"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "chit-chat/grpc/pb"
	"chit-chat/server/utils"
)

func createMessage(abcd string, maxSize int) string {
	var ind int
	var message string
	addrAbcd := []rune(abcd)

	// max size of messages is set to 64 for shorter log messages
	for range maxSize/2 {
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

	client := pb.NewSServiceClient(conn)

	clientID := int32(time.Now().Unix() % 1000)

	joinReq := &pb.JoinRequest{
		ClientId: clientID,
	}

	stream, err := client.Join(context.Background(), joinReq)
	if err != nil {
		msg := fmt.Sprintf("Failed to connect: %v", err)
		utils.LogMessage(int(clientID), -1, utils.CLIENT, utils.ERROR, msg)
	}

	utils.LogMessage(int(clientID), -1, utils.CLIENT, utils.JOIN, "Joined chit-chat")

	abcd := "abcdefghijklmnopqrstovwxyz"
	clientMsg := createMessage(abcd, 128)
	msgReq := &pb.SendMessage{
		ClientId: clientID,
		Message: clientMsg,
	} 

	msgStream, err := client.Message(context.Background(), msgReq)
	if err != nil {
		msg := fmt.Sprintf("Failed to send message: %v", err)
		utils.LogMessage(int(clientID), -1, utils.CLIENT, utils.ERROR, msg)
	}
	defer msgStream.CloseSend()

	go func() {
		for {
			reply, err := stream.Recv()
			if err == io.EOF {
				utils.LogMessage(int(clientID), -1, utils.CLIENT, utils.STREAM_END, " Server closed stream")
				return
			}
			if err != nil {
				msg := fmt.Sprintf("Stream receive error: %v", err)
				utils.LogMessage(int(clientID), -1, utils.CLIENT, utils.ERROR, msg)
				return
			}
			msg := fmt.Sprintf("\"%s\"", reply.Ack)
			utils.LogMessage(int(clientID), -1, utils.CLIENT, utils.MESSAGE_RECEIVED, msg)
		}
	}()

	time.Sleep(30 * time.Second)

	leaveReq := &pb.LeaveRequest{
		ClientId: clientID,
	}

	leaveStream, err := client.Leave(context.Background(), leaveReq)
	if err != nil {
		msg := fmt.Sprintf("Leave failed: %v", err)
		utils.LogMessage(int(clientID), -1, utils.CLIENT, utils.ERROR, msg)
	}
	defer leaveStream.CloseSend()

	utils.LogMessage(int(clientID), -1, utils.CLIENT, utils.LEAVE, "Leaving chit-chat")
	time.Sleep(1 * time.Second)

	utils.LogMessage(int(clientID), -1, utils.CLIENT, utils.SHUTDOWN, "Client exiting cleanly")
}