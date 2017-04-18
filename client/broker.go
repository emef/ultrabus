package client

import (
	"io"

	"github.com/emef/ultrabus/core"
	"github.com/emef/ultrabus/pb"
	"golang.org/x/net/context"
)

type TopicBroker struct {
	topic                string
	clientID             *pb.ClientID
	partitionIDToClients map[pb.PartitionID]pb.UltrabusNodeClient
}

type BrokeredSubscription struct {
	messages chan *pb.MessageWithOffset
	done     chan interface{}
}

func NewTopicBroker(
	topic string,
	clientID *pb.ClientID,
	partitionIDToClients map[pb.PartitionID]pb.UltrabusNodeClient) *TopicBroker {

	return &TopicBroker{topic, clientID, partitionIDToClients}
}

func (broker *TopicBroker) Subscribe() (Subscription, error) {
	// TODO queue size?
	messages := make(chan *pb.MessageWithOffset, 1)
	done := make(chan interface{}, 1)

	for partitionID, client := range broker.partitionIDToClients {
		request := &pb.SubscribeRequest{
			ClientID:    broker.clientID,
			PartitionID: &partitionID}

		stream, err := client.Subscribe(context.Background(), request)
		if err != nil {
			close(done)
			return nil, err
		}

		go func() {
			for {
				select {
				case <-done:
					break

				default:
					// NOTE: This can block us forever and halt cleanup
					in, err := stream.Recv()
					if err == io.EOF {
						break
					}

					for _, msg := range in.Messages {
						select {
						case messages <- msg:
						case <-done:
							break
						}
					}
				}
			}
		}()
	}

	return &BrokeredSubscription{messages, done}, nil
}

func (broker *TopicBroker) Publish(messages []*pb.Message) error {
	partitionIDToMessages := make(map[pb.PartitionID][]*pb.Message)
	partitions := int32(len(broker.partitionIDToClients))

	for _, message := range messages {
		partition, err := core.HashToPartition(message, partitions)
		if err != nil {
			return err
		}

		partitionID := pb.PartitionID{
			Topic:     broker.topic,
			Partition: partition}

		// Check that we have a client for every partition
		if _, ok := broker.partitionIDToClients[partitionID]; !ok {
			return &core.PartitionNotFoundError{&partitionID}
		}

		partitionMessages, _ := partitionIDToMessages[partitionID]
		partitionIDToMessages[partitionID] = append(partitionMessages, message)
	}

	for partitionID, partitionMessages := range partitionIDToMessages {
		// NOTE: have to copy the partitionId since it's a range key
		// NOTE: ^^ this might be false
		request := &pb.PublishRequest{
			PartitionID: &pb.PartitionID{
				Topic:     partitionID.Topic,
				Partition: partitionID.Partition},
			Messages: partitionMessages}

		client, _ := broker.partitionIDToClients[partitionID]
		if _, err := client.Publish(context.Background(), request); err != nil {
			return err
		}
	}

	return nil
}

func (subscription *BrokeredSubscription) Messages() chan *pb.MessageWithOffset {
	return subscription.messages
}

func (subscription *BrokeredSubscription) Stop() {
	close(subscription.done)
}
