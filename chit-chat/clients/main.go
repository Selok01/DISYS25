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

func createMessage(abcd string, maxSize int) string {
	var ind int
	var message string
	addrAbcd := []rune(abcd)

	// max size of messages is set to 64 for shorter log messages
	for range maxSize / 2 {
		ind = random.Intn(len(addrAbcd))
		message += string(addrAbcd[ind])
	}

	return message
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

func Inc_clock(s []int32, clientid int) {
	s[clientid]++
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

	Inc_clock(clientClock, int(clientID))
	utils.LogMessage(int(clientID), int(clientClock[clientID]), utils.CLIENT, utils.JOIN, "Joined chit-chat")

	stream, err := client.Chat(context.Background())
	if err != nil {
		msg := fmt.Sprintf("Failed to connect: %v", err)
		utils.LogMessage(int(clientID), -1, utils.CLIENT, utils.ERROR, msg)
		return
	}

	joinReq := &pb.ClientRequest{
		Payload: &pb.ClientRequest_Join{
			Join: &pb.JoinRequest{
				ClientId: clientID,
				VecClock: vc,
			},
		},
	}
	Inc_clock(clientClock, int(clientID))
	utils.LogMessage(int(clientID), int(clientClock[clientID]), utils.CLIENT, utils.JOIN, "Joined chit-chat")
	if err := stream.Send(joinReq); err != nil {
		utils.LogMessage(int(clientID), -1, utils.CLIENT, utils.ERROR, fmt.Sprintf("Failed to send join: %v", err))
		return
	}

	// ----- Receive messages in background -----
	go func() {
		for {
			reply, err := stream.Recv()
			if err == io.EOF {
				utils.LogMessage(int(clientID), -1, utils.CLIENT, utils.STREAM_END, "Server closed stream")
				return
			}
			if err != nil {
				utils.LogMessage(int(clientID), -1, utils.CLIENT, utils.ERROR, fmt.Sprintf("Stream receive error: %v", err))
				return
			}

			clientClock = compare_clock(reply.VecClock.Clock, clientClock)
			Inc_clock(clientClock, int(clientID))
			utils.LogMessage(int(clientID), int(clientClock[clientID]), utils.CLIENT, utils.MESSAGE_RECEIVED, fmt.Sprintf("\"%s\"", reply.Ack))
		}
	}()

	// ----- Send message -----
	abcd := "abcdefghijklmnopqrstovwxyz"
	clientMsg := createMessage(abcd, 128)
	msgReq := &pb.ClientRequest{
		Payload: &pb.ClientRequest_Message{
			Message: &pb.SendMessage{
				ClientId: clientID,
				Message:  clientMsg,
				VecClock: vc,
			},
		},
	}
	Inc_clock(clientClock, int(clientID))
	if err := stream.Send(msgReq); err != nil {
		utils.LogMessage(int(clientID), -1, utils.CLIENT, utils.ERROR, fmt.Sprintf("Failed to send message: %v", err))
	}

	// ----- Sleep to simulate work -----
	time.Sleep(30 * time.Second)

	// ----- Send Leave -----
	leaveReq := &pb.ClientRequest{
		Payload: &pb.ClientRequest_Leave{
			Leave: &pb.LeaveRequest{
				ClientId: clientID,
				VecClock: vc,
			},
		},
	}
	Inc_clock(clientClock, int(clientID))
	if err := stream.Send(leaveReq); err != nil {
		utils.LogMessage(int(clientID), -1, utils.CLIENT, utils.ERROR, fmt.Sprintf("Failed to send leave: %v", err))
	}

	utils.LogMessage(int(clientID), -1, utils.CLIENT, utils.LEAVE, "Leaving chit-chat")
	time.Sleep(1 * time.Second)

	utils.LogMessage(int(clientID), -1, utils.CLIENT, utils.SHUTDOWN, "Client exiting cleanly")
}
