package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"zmq/demo1/proto"

	zmq "github.com/pebbe/zmq4"
	"google.golang.org/grpc"
)

type server struct {
    proto.UnimplementedMessageServiceServer
    publisher *zmq.Socket
}

func (s *server) SendMessage(ctx context.Context, req *proto.Message) (*proto.Response, error) {
    _, err := s.publisher.Send(req.Content, 0)
    if err != nil {
        return nil, err
    }
    return &proto.Response{Result: "Message published successfully"}, nil
}

func main() {
    publisher, err := zmq.NewSocket(zmq.PUB)
    if err != nil {
        log.Fatalf("Failed to create ZMQ publisher: %v", err)
    }
    defer publisher.Close()

    err = publisher.Bind("tcp://*:5555")
    if err != nil {
        log.Fatalf("Failed to bind ZMQ publisher: %v", err)
    }

    lis, err := net.Listen("tcp", ":50051")
    if err != nil {
        log.Fatalf("Failed to listen: %v", err)
    }

    s := grpc.NewServer()
    proto.RegisterMessageServiceServer(s, &server{publisher: publisher})

    fmt.Println("Server is running...")
    if err := s.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}