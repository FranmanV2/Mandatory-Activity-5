package main

import (
	pb "github.com/FranmanV2/Mandatory-Activity-5/proto"
)

type AuctionState interface {
	ApplyBid(bidderID string, amount int32) pb.Outcome
	Result() (over bool, highestBid int32, highestBidder string)
	Snapshot() *pb.StateUpdate
	Restore(*pb.StateUpdate)
}
