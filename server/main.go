package main

import (
	"flag"
	"fmt"
	"strings"
)

func main() {
	id := flag.Int("id", 1, "this node's ID (1-3)")
	port := flag.Int("port", 5001, "port to listen on")
	peers := flag.String("peers", "", "comma-separated peer addresses, e.g. localhost:5002,localhost:5003")
	flag.Parse()

	peerList := strings.Split(*peers, ",")
	fmt.Printf("Node %d starting on port %d, peers: %v\n", *id, *port, peerList)
	// TODO: create AuctionState, start gRPC servers
}
