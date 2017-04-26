package core

import (
	"github.com/emef/ultrabus/pb"
	"google.golang.org/grpc"
)

type ConnectionManager interface {
	GetReadClient(meta *pb.TopicMeta, partition int32) (pb.UltrabusNodeClient, error)
	GetWriteClient(meta *pb.TopicMeta, partition int32) (pb.UltrabusNodeClient, error)
}

type singleAddrConnectionManager struct {
	client pb.UltrabusNodeClient
}

func NewSingleAddrConnectionManager(serverAddr string) (ConnectionManager, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	client := pb.NewUltrabusNodeClient(conn)
	manager := &singleAddrConnectionManager{client}
	return manager, nil
}

func (manager *singleAddrConnectionManager) GetReadClient(
	meta *pb.TopicMeta, partition int32) (pb.UltrabusNodeClient, error) {

	return manager.client, nil
}

func (manager *singleAddrConnectionManager) GetWriteClient(
	meta *pb.TopicMeta,  partition int32) (pb.UltrabusNodeClient, error) {

	return manager.client, nil
}
