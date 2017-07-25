package main

import (
	"flag"
	"fmt"
	"net"

	"github.com/emef/ultrabus"
	"github.com/emef/ultrabus/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

var (
	port = flag.Int("port", 10000, "The server port")
)

func main() {
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		grpclog.Fatalf("failed to listen: %v", err)
	}

	server := ultrabus.NewNodeService()

	grpcServer := grpc.NewServer()
	pb.RegisterUltrabusNodeServer(grpcServer, server)
	grpcServer.Serve(lis)
}
