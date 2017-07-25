package ultrabus

import (
	"math/rand"

	"github.com/emef/ultrabus/pb"
	"google.golang.org/grpc"
)

type ConnectionManager interface {
	GetReadClient(partitionId *pb.PartitionID) (pb.UltrabusNodeClient, error)
	GetWriteClient(partitionId *pb.PartitionID) (pb.UltrabusNodeClient, error)
}

type connectedClient struct {
	serverAddr string
	conn *grpc.ClientConn
	client pb.UltrabusNodeClient
}

type discoveryConnectionManager struct {
	readClients map[pb.PartitionID]*connectedClient
	writeClients map[pb.PartitionID]*connectedClient
	discovery Discovery
}

func NewDiscoveryConnectionManager(discovery Discovery) ConnectionManager {
	return &discoveryConnectionManager{
		readClients: make(map[pb.PartitionID]*connectedClient),
		writeClients: make(map[pb.PartitionID]*connectedClient),
		discovery: discovery}
}

func (manager *discoveryConnectionManager) GetReadClient(
	partitionId *pb.PartitionID) (pb.UltrabusNodeClient, error) {

	connClient, exists := manager.readClients[*partitionId]
	if exists && connClient.conn.GetState() != grpc.Shutdown {
		return connClient.client, nil
	}

	addrs, err := manager.discovery.GetPartitionAddrs(partitionId)
	if err != nil {
		return nil, err
	}

	serverAddr := addrs[rand.Intn(len(addrs))]
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	client := pb.NewUltrabusNodeClient(conn)
	manager.readClients[*partitionId] = &connectedClient{serverAddr, conn, client}

	return client, nil
}

func (manager *discoveryConnectionManager) GetWriteClient(
	partitionId *pb.PartitionID) (pb.UltrabusNodeClient, error) {

	serverAddr, err := manager.discovery.GetLeaderAddr(partitionId)
	if err != nil {
		return nil, err
	}

	connClient, exists := manager.writeClients[*partitionId]
	if exists && connClient.conn.GetState() != grpc.Shutdown {
		if connClient.serverAddr == serverAddr {
			return connClient.client, nil
		} else {
			connClient.conn.Close()
		}
	}

	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	client := pb.NewUltrabusNodeClient(conn)
	manager.writeClients[*partitionId] = &connectedClient{serverAddr, conn, client}

	return client, nil
}
