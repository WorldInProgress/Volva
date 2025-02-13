package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"zmq/demo1/proto"

	zmq "github.com/pebbe/zmq4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
    // 创建gRPC客户端连接
    conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }
    defer conn.Close()

    client := proto.NewMessageServiceClient(conn)

    // 创建ZMQ订阅者
    subscriber, err := zmq.NewSocket(zmq.SUB)
    if err != nil {
        log.Fatalf("Failed to create subscriber: %v", err)
    }
    defer subscriber.Close()

    err = subscriber.Connect("tcp://localhost:5555")
    if err != nil {
        log.Fatalf("Failed to connect subscriber: %v", err)
    }

    // 订阅所有消息
    subscriber.SetSubscribe("")

    // 启动一个goroutine来接收ZMQ消息
    go func() {
        for {
            msg, err := subscriber.Recv(0)
            if err != nil {
                log.Printf("Error receiving message: %v", err)
                continue
            }
            fmt.Printf("Received via ZMQ: %s\n", msg)
        }
    }()

    // 通过gRPC发送消息
    for i := 0; i < 5; i++ {
        message := fmt.Sprintf("Hello %d", i)
        resp, err := client.SendMessage(context.Background(), &proto.Message{Content: message})
        if err != nil {
            log.Printf("Error sending message: %v", err)
            continue
        }
        fmt.Printf("gRPC response: %s\n", resp.Result)
        time.Sleep(time.Second)
    }

    // 等待一会儿以确保收到所有ZMQ消息
    time.Sleep(time.Second * 2)
}