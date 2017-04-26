package client

import (
	"io"
	"sync"
	"time"

	"github.com/emef/ultrabus/core"
	"github.com/emef/ultrabus/pb"
	"golang.org/x/net/context"
)

type TopicBroker struct {
	topic                *pb.TopicMeta
	clientID             *pb.ClientID
	connectionManager core.ConnectionManager
}

type BrokeredSubscription struct {
	messages chan *pb.MessageWithOffset
	done     chan interface{}
}

func NewTopicBroker(
	topic *pb.TopicMeta,
	clientID *pb.ClientID,
	connectionManager core.ConnectionManager) *TopicBroker {
	return &TopicBroker{topic, clientID, connectionManager}
}

func (broker *TopicBroker) Subscribe() (Subscription, error) {
	// TODO queue size?
	messages := make(chan *pb.MessageWithOffset, 1)
	done := make(chan interface{}, 1)

	var wg sync.WaitGroup
	wg.Add(int(broker.topic.Partitions))

	for partition := int32(0); partition < broker.topic.Partitions; partition++ {
		go func(partition int32) {
			request := &pb.SubscribeRequest{
				ClientID:    broker.clientID,
				PartitionID: &pb.PartitionID{
					Topic: broker.topic.Topic,
					Partition: partition}}

			var stream pb.UltrabusNode_SubscribeClient = nil

			for {
				select {
				case <-done:
					wg.Done()
					return

				default:
					if stream == nil {
						client, err := broker.connectionManager.GetReadClient(
							broker.topic, partition)

						if err == nil {
							stream, err = client.Subscribe(context.Background(), request)
						}

						if err != nil {
							// TODO LOG?
							time.Sleep(time.Second)
							break
						}
					}

					// NOTE: This can block us forever and halt cleanup
					in, err := stream.Recv()
					if err == io.EOF || err != nil {
						// TODO: differentiate between EOF and other error?
						// TODO: move this whole goroutine out of broker (ManagedSubscription?)
						stream = nil
						break
					}

					for _, msg := range in.Messages {
						select {
						case messages <- msg:
						case <-done:
							wg.Done()
							return
						}
					}
				}
			}
		}(partition)
	}

	go func() {
		wg.Wait()
		close(done)
		close(messages)
	}()

	return &BrokeredSubscription{messages, done}, nil
}

// TODO: this could be moved out of broker as well
func (broker *TopicBroker) Publish(messages []*pb.Message) error {
	partitionIDToMessages := make(map[pb.PartitionID][]*pb.Message)
	partitions := broker.topic.Partitions

	for _, message := range messages {
		partition, err := core.HashToPartition(message, partitions)
		if err != nil {
			return err
		}

		partitionID := pb.PartitionID{
			Topic:     broker.topic.Topic,
			Partition: partition}

		partitionMessages, _ := partitionIDToMessages[partitionID]
		partitionIDToMessages[partitionID] = append(partitionMessages, message)
	}

	var wg sync.WaitGroup
	wg.Add(len(partitionIDToMessages))

	for partitionID, partitionMessages := range partitionIDToMessages {
		request := &pb.PublishRequest{
			PartitionID: &pb.PartitionID{
				Topic:     partitionID.Topic,
				Partition: partitionID.Partition},
			Messages: partitionMessages}

		go func(request *pb.PublishRequest) {
			for {
				client, err := broker.connectionManager.GetWriteClient(
					broker.topic, request.PartitionID.Partition)

				if err == nil {
					_, err = client.Publish(context.Background(), request)
				}

				if err != nil {
					time.Sleep(time.Second)
				} else {
					wg.Done()
					return
				}
			}
		}(request)
	}

	wg.Wait()

	return nil
}

func (subscription *BrokeredSubscription) Messages() chan *pb.MessageWithOffset {
	return subscription.messages
}

func (subscription *BrokeredSubscription) Stop() {
	close(subscription.done)
}
