package client

import (
	"github.com/emef/ultrabus/pb"
	"google.golang.org/grpc"
)

type UltrabusClient interface {
	Subscribe(topic string) (Subscription, error)
	Publish(topic string, messages []*pb.Message) error
}

type Subscription interface {
	Messages() chan *pb.MessageWithOffset
	Stop()
}

type singleAddrBrokeredClient struct {
	clientID *pb.ClientID
	client   pb.UltrabusNodeClient
	brokers  map[string]*TopicBroker
}

func NewSingleAddrBrokeredClient(
	consumerGroup string,
	serverAddr string) (UltrabusClient, error) {

	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	client := pb.NewUltrabusNodeClient(conn)

	clientID := &pb.ClientID{
		ConsumerGroup: consumerGroup,
		ConsumerID:    ""}

	brokers := make(map[string]*TopicBroker)

	return &singleAddrBrokeredClient{clientID, client, brokers}, nil
}

func (client *singleAddrBrokeredClient) broker(
	topic string) *TopicBroker {

	if _, ok := client.brokers[topic]; !ok {
		// TODO: use real etcd partition metadata
		partitions := 10

		partitionIDToClients := make(map[pb.PartitionID]pb.UltrabusNodeClient)
		for i := 0; i < partitions; i++ {
			partitionID := pb.PartitionID{Topic: topic, Partition: int32(i)}
			partitionIDToClients[partitionID] = client.client
		}

		broker := NewTopicBroker(topic, client.clientID, partitionIDToClients)
		client.brokers[topic] = broker
	}

	return client.brokers[topic]
}

func (client *singleAddrBrokeredClient) Subscribe(
	topic string) (Subscription, error) {

	return client.broker(topic).Subscribe()
}

func (client *singleAddrBrokeredClient) Publish(
	topic string, messages []*pb.Message) error {

	// NOTE: this is only ok in single address world, with replicas
	// you would need to make sure to publish to the master, etc.
	return client.broker(topic).Publish(messages)
}
