package node

import (
	"github.com/emef/ultrabus/core"
	"github.com/emef/ultrabus/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
)

type NodeService struct {
	partitions map[pb.PartitionID]*Partition
}

func (node *NodeService) Subscribe(
	request *pb.SubscribeRequest,
	stream pb.UltrabusNode_SubscribeServer) error {

	grpclog.Printf("Connection opened from {%v}\n", request.ClientID)

	partition, ok := node.partitions[*request.PartitionID]
	if !ok {
		return &core.PartitionNotFoundError{request.PartitionID}
	}

	handle, err := partition.RegisterConsumer(request.ClientID, stream)
	if err != nil {
		return err
	}

	return handle.Wait()
}

func (node *NodeService) Publish(
	context context.Context,
	request *pb.PublishRequest) (*pb.PublishResponse, error) {

	partition, ok := node.partitions[*request.PartitionID]
	if !ok {
		return nil, &core.PartitionNotFoundError{request.PartitionID}
	}

	offsets := make([]int64, len(request.Messages))
	for i, msg := range request.Messages {
		offset, err := partition.Append(msg)
		if err != nil {
			// TODO: rollback??
			grpclog.Printf("Error appending to partition: %v", err)
			return nil, err
		}

		offsets[i] = offset
	}

	return &pb.PublishResponse{Offsets: offsets}, nil
}

func (node *NodeService) CreateTopic(
	context context.Context,
	request *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {

	// TODO:
	//   1. check if exists: error if so
	//   2. add entry in etcd

	topic := request.Meta.Topic
	partitions := request.Meta.Partitions
	for i := int32(0); i < partitions; i++ {
		partitionID := pb.PartitionID{Topic: topic, Partition: i}
		node.partitions[partitionID] = NewInMemoryPartition()
	}

	return &pb.CreateTopicResponse{Ok: true}, nil
}

func NewNodeService() pb.UltrabusNodeServer {
	partitions := make(map[pb.PartitionID]*Partition)
	node := &NodeService{partitions}
	node.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		&pb.TopicMeta{Topic: "topic", Partitions: 10}})

	return node
}
