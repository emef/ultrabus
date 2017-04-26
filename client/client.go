package client

import (
	"code.google.com/p/go-uuid/uuid"
	"github.com/emef/ultrabus/pb"
	"github.com/emef/ultrabus/core"
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
	connectionManager core.ConnectionManager
	brokers  map[string]*TopicBroker
}

func NewSingleAddrBrokeredClient(
	consumerGroup string,
	serverAddr string) (UltrabusClient, error) {

	connectionManager, _ := core.NewSingleAddrConnectionManager(serverAddr)

	clientID := &pb.ClientID{
		ConsumerGroup: consumerGroup,
		ConsumerID:    uuid.New()}

	brokers := make(map[string]*TopicBroker)

	return &singleAddrBrokeredClient{clientID, connectionManager, brokers}, nil
}

func (client *singleAddrBrokeredClient) broker(
	topic string) *TopicBroker {

	// TODO: lookup topic meta
	meta := &pb.TopicMeta{Topic: topic, Partitions: int32(10)}

	if _, ok := client.brokers[meta.Topic]; !ok {
		broker := NewTopicBroker(meta, client.clientID, client.connectionManager)
		client.brokers[meta.Topic] = broker
	}

	return client.brokers[meta.Topic]
}

func (client *singleAddrBrokeredClient) Subscribe(
	topic string) (Subscription, error) {

	return client.broker(topic).Subscribe()
}

func (client *singleAddrBrokeredClient) Publish(
	topic string, messages []*pb.Message) error {

	return client.broker(topic).Publish(messages)
}
