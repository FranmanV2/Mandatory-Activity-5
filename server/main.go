package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	pb "github.com/FranmanV2/Mandatory-Activity-5/proto"
	"google.golang.org/grpc"
)

func main() {
	id := flag.Int("id", 1, "this node's ID (1-3)")
	port := flag.Int("port", 5001, "port to listen on")
	peers := flag.String("peers", "", "comma-separated peer addresses, e.g. localhost:5002,localhost:5003")
	duration := flag.Duration("duration", 100*time.Second, "auction length (1 time unit = 1 second)")
	flag.Parse()

	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	log.SetPrefix(fmt.Sprintf("[node %d] ", *id))

	// Each node creates its own deadline at startup. The primary's deadline
	// is replicated to the backups in the first StateUpdate, which overwrites
	// theirs (see Restore in auction.go).
	auction := NewAuction(time.Now().Add(*duration))

	// One failure detector per node, shared by both services.
	fd := newFailureDetector(*id, peerList)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("cannot listen on port %d: %v", *port, err)
	}

	srv := grpc.NewServer()
	pb.RegisterAuctionServer(srv, newAuctionServer(auction, *id, peerList, fd))         // Person 2
	pb.RegisterReplicationServer(srv, newReplicationServer(auction, *id, peerList, fd)) // Person 2 (Replicate) + Person 3 (Heartbeat)

	log.Printf("starting on port %d, peers %v, auction ends in %v", *port, peerList, *duration)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("server stopped: %v", err)
	}
}
