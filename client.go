package ultrabus

import (
	"code.google.com/p/go-uuid/uuid"
	"github.com/emef/ultrabus/pb"
)

type UltrabusClient interface {
	Subscribe(topic string) (Subscription, error)
	Publish(topic string, messages []*pb.Message) error
  Create(topic string, partitions int32, replicas int32) error
}

type Subscription interface {
	Messages() chan *pb.MessageWithOffset
	Stop()
}

type singleAddrBrokeredClient struct {
	clientID *pb.ClientID
	discovery Discovery
	connectionManager ConnectionManager
	brokers  map[string]*TopicBroker
}

func NewSingleAddrBrokeredClient(
	consumerGroup string,
	discovery Discovery) (UltrabusClient, error) {

	connectionManager, _ := NewSingleAddrConnectionManager(discovery)

	clientID := &pb.ClientID{
		ConsumerGroup: consumerGroup,
		ConsumerID:    uuid.New()}

	brokers := make(map[string]*TopicBroker)

	return &singleAddrBrokeredClient{
		clientID, discovery, connectionManager, brokers}, nil
}

func (client *singleAddrBrokeredClient) broker(topic string) (*TopicBroker, error) {
	if _, ok := client.brokers[topic]; !ok {
		meta, err := client.discovery.GetTopic(topic)
		if err != nil {
			return nil, err
		}

		broker := NewTopicBroker(meta, client.clientID, client.connectionManager)
		client.brokers[meta.Topic] = broker
	}

	return client.brokers[topic], nil
}

func (client *singleAddrBrokeredClient) Subscribe(
	topic string) (Subscription, error) {

	broker, err := client.broker(topic)
	if err != nil {
		return nil, err
	}

	return broker.Subscribe()
}

func (client *singleAddrBrokeredClient) Publish(
	topic string, messages []*pb.Message) error {

	broker, err := client.broker(topic)
	if err != nil {
		return err
	}

	return broker.Publish(messages)
}

func (client *singleAddrBrokeredClient) Create(
	topic string, partitions int32, replicas int32) error {

	return client.discovery.CreateTopic(&pb.TopicMeta{
		Topic: topic, Partitions: partitions, Replicas: replicas})
}
