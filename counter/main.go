package main

import (
	"flag"

	"net"

	"golang.org/x/net/context"

	log "github.com/Sirupsen/logrus"
	"github.com/margic/gointro/counter/protos"
	"google.golang.org/grpc"
)

var debug bool

func main() {
	flag.BoolVar(&debug, "debug", false, "enable debug")
	flag.Parse()

	if debug {
		log.SetLevel(log.DebugLevel)
	}
	log.Info("starting counter service")
	lis, err := net.Listen("tcp", ":8079")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	protos.RegisterEventCountServer(grpcServer, &CounterServer{})
	log.Debugf("starting grpc listener on: %s", lis.Addr().String())
	grpcServer.Serve(lis)
}

// CounterServer grpc server implementation
type CounterServer struct {
}

// Count counts events
func (cs *CounterServer) Count(ctx context.Context, event *protos.EventIn) (*protos.Empty, error) {
	log.WithField("content", event.Content).Debug("Received")
	return &protos.Empty{}, nil
}
