package main

import (
	"flag"

	"github.com/emef/ultrabus"
	"google.golang.org/grpc/grpclog"
)

var (
	serverAddr = flag.String("server_addr", "127.0.0.1:10000",
		"The server address in the format of host:port")
	topic         = flag.String("topic", "topic", "Topic to subscribe to")
	partitions    = flag.Int("partitions", 10, "Number of partitions (temporary)")
	numMessages   = flag.Int("n", 10, "Number of messages to read")
	consumerGroup = flag.String("consumer_group", "grp", "Consumer group name")
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

	subscription, err := client.Subscribe(*topic)
	if err != nil {
		grpclog.Fatalf("Failed to subscribe to topic %v: %v", *topic, err)
	}

	for i := 0; i < *numMessages; i++ {
		msg := <-subscription.Messages()
		if msg == nil {
			grpclog.Fatalf("Failed to receive message")
		}

		grpclog.Printf("%v", msg)
	}

	subscription.Stop()
}
