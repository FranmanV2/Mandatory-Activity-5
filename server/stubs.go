package main

import (
	"context"

	pb "github.com/FranmanV2/Mandatory-Activity-5/proto"
)

// ---- TEMPORARY: replace with real implementations ----

type auctionServer struct {
	pb.UnimplementedAuctionServer
	state AuctionState
}

// Person 2: replace this with the real handler (apply, replicate, reply / forward to primary).
func newAuctionServer(state AuctionState, id int, peers []string) *auctionServer {
	return &auctionServer{state: state}
}

func (s *auctionServer) Bid(ctx context.Context, req *pb.BidRequest) (*pb.BidReply, error) {
	return &pb.BidReply{Outcome: s.state.ApplyBid(req.BidderId, req.Amount)}, nil
}

func (s *auctionServer) Result(ctx context.Context, req *pb.ResultRequest) (*pb.ResultReply, error) {
	over, bid, who := s.state.Result()
	return &pb.ResultReply{Over: over, HighestBid: bid, HighestBidder: who}, nil
}

type replicationServer struct {
	pb.UnimplementedReplicationServer
	state AuctionState
}

// Person 2: Replicate. Person 3: Heartbeat.
func newReplicationServer(state AuctionState, id int, peers []string) *replicationServer {
	return &replicationServer{state: state}
}

func (s *replicationServer) Replicate(ctx context.Context, u *pb.StateUpdate) (*pb.Ack, error) {
	s.state.Restore(u)
	return &pb.Ack{}, nil
}

func (s *replicationServer) Heartbeat(ctx context.Context, p *pb.Ping) (*pb.Pong, error) {
	return &pb.Pong{}, nil
}
