package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/emef/ultrabus"
	"github.com/emef/ultrabus/pb"
	"google.golang.org/grpc/grpclog"
)

var (
	serverAddr = flag.String("server_addr", "127.0.0.1:10000",
		"The server address in the format of host:port")
	topic              = flag.String("topic", "topic", "Topic to subscribe to")
	numMessages        = flag.Int("n", 10, "Number of messages to publish")
	messagesPerRequest = flag.Int("messages_per_request", 1,
		"Messages to batch in each request")
	intervalSeconds = flag.Int("interval_seconds", 1, "Seconds between publishing")
	consumerGroup   = flag.String("consumer_group", "", "Consumer group name")
)

func main() {
	flag.Parse()

	discovery, err := ultrabus.NewSingleAddrDiscovery(*serverAddr)
	if err != nil {
		grpclog.Fatalf("Failed to create discovery: %v", err)
	}

	client, err := ultrabus.NewSingleAddrBrokeredClient(
		*consumerGroup, discovery)
	if err != nil {
		grpclog.Fatalf("Failed to create brokered client: %v", err)
	}

	for i := 0; i < *numMessages; i++ {
		messages := make([]*pb.Message, 0)
		for j := 0; j < *messagesPerRequest; j++ {
			id := j + i*(*messagesPerRequest)
			message := &pb.Message{
				Key:   []byte(fmt.Sprintf("key-%v", id)),
				Value: []byte(fmt.Sprintf("value-%v", id))}
			messages = append(messages, message)
		}

		if err := client.Publish(*topic, messages); err != nil {
			grpclog.Fatalf("Could not publish to topic %v: %v", *topic, err)
		}

		time.Sleep(time.Duration(*intervalSeconds) * time.Second)
	}
}
