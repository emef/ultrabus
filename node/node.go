package node

import (
	"github.com/emef/ultrabus/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
)

type NodeService struct {
	partition *Partition}

func (node *NodeService) Subscribe(
	request *pb.SubscribeRequest,
	stream pb.UltrabusNode_SubscribeServer) error {

	grpclog.Printf("Connection opened from %v\n", request.ClientID)

	handle, err := node.partition.RegisterConsumer(request.ClientID, stream)
	if err != nil {
		return err
	}

	return handle.Wait()
}

func (node *NodeService) Publish(
	context context.Context,
	request *pb.PublishRequest) (*pb.PublishResponse, error) {

	offsets := make([]int64, len(request.Messages))
	for i, msg := range request.Messages {
		offset, err := node.partition.Append(msg)
		if err != nil {
			// TODO: rollback??
			grpclog.Printf("Error appending to partition: %v", err)
			return nil, err
		}

		offsets[i] = offset
	}

	return &pb.PublishResponse{Offsets: offsets}, nil
}


func NewNodeService() pb.UltrabusNodeServer {
	partition := NewInMemoryPartition()

	return &NodeService{partition}
}
