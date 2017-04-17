package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/emef/ultrabus/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

var (
	serverAddr = flag.String("server_addr", "127.0.0.1:10000",
		"The server address in the format of host:port")
	topic = flag.String("topic", "", "Topic to subscribe to")
	numMessages = flag.Int("n", 10, "Number of messages to publish")
	messagesPerRequest = flag.Int("messages_per_request", 1,
		"Messages to batch in each request")
	intervalSeconds = flag.Int("interval_seconds", 1, "Seconds between publishing")
	consumerGroup = flag.String("consumer_group", "", "Consumer group name")
	consumerID = flag.String("consumer_id", "", "Unique consumer ID")
)

func main() {
	flag.Parse()

	conn, err := grpc.Dial(*serverAddr, grpc.WithInsecure())
	if err != nil {
		grpclog.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewUltrabusNodeClient(conn)

	request := &pb.PublishRequest{Topic: *topic}

	for i := 0; i < *numMessages; i++ {
		messages := make([]*pb.Message, 0)
		for j := 0; j < *messagesPerRequest; j++ {
			id := j + i * (*messagesPerRequest)
			message := &pb.Message{
				Key: []byte("ultrabus_publisher"),
				Value: []byte(fmt.Sprintf("value-%v", id))}
			messages = append(messages, message)
		}

		request.Messages = messages
		_, err := client.Publish(context.Background(), request)
		if err != nil {
			grpclog.Fatalf("Could not publish to topic %v: %v", *topic, err)
		}

		time.Sleep(time.Duration(*intervalSeconds) * time.Second)
	}
}
