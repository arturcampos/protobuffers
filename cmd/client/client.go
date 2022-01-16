package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/arturcampos/protobuffers/pb"
	"google.golang.org/grpc"
)

func main() {
	connection, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect to gRPC Server: %v", err)
	}

	defer connection.Close()

	client := pb.NewUserServiceClient(connection)
	//AddUser(client)
	//AddUserVerbose(client)
	//AddUsers(client)
	AddUserStreamBidirectional(client)

}

func AddUser(client pb.UserServiceClient) {
	req := &pb.User{
		Id:    "0",
		Name:  "José",
		Email: "jose@jose.com",
	}

	res, err := client.AddUser(context.Background(), req)
	if err != nil {
		log.Fatalf("Could not make gRPC request: %v", err)
	}

	fmt.Println(res)
}

func AddUserVerbose(client pb.UserServiceClient) {
	req := &pb.User{
		Id:    "0",
		Name:  "José",
		Email: "jose@jose.com",
	}

	resStream, err := client.AddUserVerbose(context.Background(), req)
	if err != nil {
		log.Fatalf("Could not make gRPC request: %v", err)
	}

	for {
		stream, err := resStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Could not receive the message: %v", err)
		}

		fmt.Println("Status", stream.Status, " - ", stream.GetUser())
	}
}

func AddUsers(client pb.UserServiceClient) {
	reqs := []*pb.User{
		{
			Id:    "a1",
			Name:  "Artur",
			Email: "artur@artur.com",
		},
		{
			Id:    "a2",
			Name:  "Artur 2",
			Email: "artur@artur.com",
		},
		{
			Id:    "a3",
			Name:  "Artur 3",
			Email: "artur@artur.com",
		},
		{
			Id:    "a4",
			Name:  "Artur 4",
			Email: "artur@artur.com",
		},
	}
	stream, err := client.AddUsers(context.Background())

	if err != nil {
		log.Fatalf("Error Creating request: %v", err)
	}

	for _, req := range reqs {
		stream.Send(req)
		time.Sleep(time.Second * 3)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Error receiving response: %v", err)
	}

	fmt.Println(res)

}

func AddUserStreamBidirectional(client pb.UserServiceClient) {

	stream, err := client.AddUserStreamBidirectional(context.Background())
	if err != nil {
		log.Fatalf("Error Creating request: %v", err)
	}

	reqs := []*pb.User{
		{
			Id:    "a1",
			Name:  "Artur",
			Email: "artur@artur.com",
		},
		{
			Id:    "a2",
			Name:  "Artur 2",
			Email: "artur@artur.com",
		},
		{
			Id:    "a3",
			Name:  "Artur 3",
			Email: "artur@artur.com",
		},
		{
			Id:    "a4",
			Name:  "Artur 4",
			Email: "artur@artur.com",
		},
	}

	wait := make(chan int)

	go func() {

		for _, req := range reqs {
			fmt.Println("Sending user: ", req.GetName())
			stream.Send(req)
			time.Sleep(time.Second * 3)
		}
		stream.CloseSend()
	}()

	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("Error receiving stream: %v", err)
				break
			}
			fmt.Printf("Receiving user %v with %v status\n", res.GetUser().GetName(), res.GetStatus())
		}
		close(wait)
	}()

	<-wait
}
