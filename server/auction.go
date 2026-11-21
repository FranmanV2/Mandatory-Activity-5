package main

import (
	"sync"
	"time"

	pb "github.com/FranmanV2/Mandatory-Activity-5/proto"
)

type AuctionState interface {
	ApplyBid(bidderID string, amount int32) pb.Outcome
	Result() (over bool, highestBid int32, highestBidder string)
	Snapshot() *pb.StateUpdate
	Restore(*pb.StateUpdate)
}

// Auction implements AuctionState.
type Auction struct {
	mu            sync.Mutex
	highestBid    int32
	highestBidder string
	bidders       map[string]int32 // bidder_id -> their latest bid
	deadline      time.Time
	version       int64
	now           func() time.Time // replaceable clock, so tests can control time
}

// Compile-time check that Auction satisfies the interface.
var _ AuctionState = (*Auction)(nil)

func NewAuction(deadline time.Time) *Auction {
	return &Auction{
		bidders:  make(map[string]int32),
		deadline: deadline,
		now:      time.Now,
	}
}

func (a *Auction) isOver() bool {
	return !a.now().Before(a.deadline)
}

func (a *Auction) ApplyBid(bidderID string, amount int32) pb.Outcome {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Retry of a bid that was already accepted: succeed again (idempotency).
	// Checked first, so a client retrying after a crash still gets its answer
	// even if the deadline passed in the meantime.
	if prev, known := a.bidders[bidderID]; known && prev == amount {
		return pb.Outcome_SUCCESS
	}

	if a.isOver() || amount <= 0 {
		return pb.Outcome_FAIL
	}

	// Must be higher than this bidder's own previous bid...
	if prev, known := a.bidders[bidderID]; known && amount <= prev {
		return pb.Outcome_FAIL
	}
	// ...and higher than the current highest bid.
	if amount <= a.highestBid {
		return pb.Outcome_FAIL
	}

	a.bidders[bidderID] = amount // first bid registers the bidder
	a.highestBid = amount
	a.highestBidder = bidderID
	a.version++
	return pb.Outcome_SUCCESS
}

func (a *Auction) Result() (bool, int32, string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.isOver(), a.highestBid, a.highestBidder
}

func (a *Auction) Snapshot() *pb.StateUpdate {
	a.mu.Lock()
	defer a.mu.Unlock()

	copied := make(map[string]int32, len(a.bidders))
	for k, v := range a.bidders {
		copied[k] = v
	}
	return &pb.StateUpdate{
		Version:        a.version,
		HighestBid:     a.highestBid,
		HighestBidder:  a.highestBidder,
		Bidders:        copied,
		DeadlineUnixMs: a.deadline.UnixMilli(),
	}
}

func (a *Auction) Restore(u *pb.StateUpdate) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if u.Version < a.version {
		return
	}
	a.highestBid = u.HighestBid
	a.highestBidder = u.HighestBidder
	a.version = u.Version
	a.bidders = make(map[string]int32, len(u.Bidders))
	for k, v := range u.Bidders {
		a.bidders[k] = v
	}
	if u.DeadlineUnixMs != 0 {
		a.deadline = time.UnixMilli(u.DeadlineUnixMs)
	}
}
