package main

import (
	"flag"
	"io"

	"github.com/emef/ultrabus/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

var (
	serverAddr = flag.String("server_addr", "127.0.0.1:10000",
		"The server address in the format of host:port")
	topic         = flag.String("topic", "", "Topic to subscribe to")
	numMessages   = flag.Int("n", 10, "Number of messages to read")
	consumerGroup = flag.String("consumer_group", "", "Consumer group name")
	consumerID    = flag.String("consumer_id", "", "Unique consumer ID")
)

func main() {
	flag.Parse()

	conn, err := grpc.Dial(*serverAddr, grpc.WithInsecure())
	if err != nil {
		grpclog.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewUltrabusNodeClient(conn)

	request := &pb.SubscribeRequest{
		ClientID: &pb.ClientID{ConsumerGroup: *consumerGroup, ConsumerID: *consumerID},
		Topic:    *topic}
	stream, err := client.Subscribe(context.Background(), request)
	if err != nil {
		grpclog.Fatalf("Could not subscribe to topic %v: %v", *topic, err)
	}

	for i := 0; i < *numMessages; i++ {
		in, err := stream.Recv()
		if err == io.EOF {
			println("eof")
			break
		} else if err != nil {
			grpclog.Fatalf("Failed to receive message: %v", err)
		}
		grpclog.Printf("%v", in)
	}

}
