package ultrabus

import (
	"time"

	"github.com/emef/ultrabus/pb"
)

type Discovery interface {
	// Node discovery
	AdvertiseNodeAddr(serverAddr string, ttl time.Duration) error
	GetAllNodeAddrs() ([]string, error)
	GetLeaderAddr(partitionID *pb.PartitionID) (string, error)
	GetReplicaAddr(partitionID *pb.PartitionID) (string, error)

	// Consumer group management
	AdvertiseConsumer(
		topic, consumerGroup, consumerID string, ttl time.Duration) error
	GetConsumers(topic string, consumerGroup string) ([]string, error)

  // TODO: subscriptions

  // Topics
  CreateTopic(topicMeta *pb.TopicMeta) error
	GetTopic(topic string) (*pb.TopicMeta, error)
}

type singleAddrDiscovery struct {
	serverAddr string
	consumers map[string](map[string][]string)
}

func NewSingleAddrDiscovery(serverAddr string) (Discovery, error) {
	return &singleAddrDiscovery{
		serverAddr,
		make(map[string](map[string][]string))}, nil
}

func (discovery *singleAddrDiscovery) AdvertiseNodeAddr(
	serverAddr string, ttl time.Duration) error {

	return nil
}

func (discovery *singleAddrDiscovery) GetAllNodeAddrs() ([]string, error) {
	return []string{discovery.serverAddr}, nil
}

func (discovery *singleAddrDiscovery) GetLeaderAddr(
	partitionID *pb.PartitionID) (string, error) {

	return discovery.serverAddr, nil
}

func (discovery *singleAddrDiscovery) GetReplicaAddr(
	partitionID *pb.PartitionID) (string, error) {

	return discovery.serverAddr, nil
}

func (discovery *singleAddrDiscovery) AdvertiseConsumer(
	topic, consumerGroup, consumerID string, ttl time.Duration) error {

	_, ok := discovery.consumers[topic]
	if !ok {
		discovery.consumers[topic] = make(map[string][]string)
	}

	discovery.consumers[topic][consumerGroup] = append(
		discovery.consumers[topic][consumerGroup], consumerID)

	return nil
}

func (discovery *singleAddrDiscovery) GetConsumers(
	topic string, consumerGroup string) ([]string, error) {

	topicGroups, ok := discovery.consumers[topic]
	if !ok {
		return nil, nil
	}

	consumers, _ := topicGroups[consumerGroup]
	return consumers, nil
}

func (discovery *singleAddrDiscovery) CreateTopic(topicMeta *pb.TopicMeta) error {
	return nil
}

func (discovery *singleAddrDiscovery) GetTopic(topic string) (*pb.TopicMeta, error) {
	return &pb.TopicMeta{Topic: topic, Partitions: 10}, nil
}
