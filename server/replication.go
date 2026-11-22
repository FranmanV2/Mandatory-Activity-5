package main

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	pb "github.com/FranmanV2/Mandatory-Activity-5/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// Replication to ONE backup. A crashed node refuses the connection almost
	// instantly; the timeout only matters for a node that hangs.
	replicateTimeout = 1 * time.Second

	// A backup forwarding to the primary must wait out the primary's own
	// replication (at most 2 x replicateTimeout), yet stay under the client's 5 s.
	forwardTimeout = 3 * time.Second

	// gRPC metadata key marking a request that has already been forwarded once.
	forwardedKey = "x-forwarded"

	promotionPoll = 200 * time.Millisecond
)

// peerConns caches one gRPC connection per peer address.
type peerConns struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func (p *peerConns) get(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	p.conns[addr] = c
	return c, nil
}

// ---------------------------------------------------------------------------
// Client-facing service: Bid and Result
// ---------------------------------------------------------------------------

type auctionServer struct {
	pb.UnimplementedAuctionServer
	state AuctionState
	id    int
	peers []string
	fd    FailureDetector
	conns *peerConns

	// Serialises "apply bid -> snapshot -> replicate -> reply" on the primary.
	// Without it two concurrent bids could be replicated out of order, and a
	// Result could observe a bid that the backups do not have yet.
	// Only ever held on the primary, and never while forwarding.
	mu sync.Mutex
}

func newAuctionServer(state AuctionState, id int, peers []string, fd FailureDetector) *auctionServer {
	s := &auctionServer{
		state: state,
		id:    id,
		peers: peers,
		fd:    fd,
		conns: &peerConns{conns: make(map[string]*grpc.ClientConn)},
	}
	go s.watchPromotion()
	return s
}

func (s *auctionServer) Bid(ctx context.Context, req *pb.BidRequest) (*pb.BidReply, error) {
	if !s.fd.IsPrimary() {
		return s.forwardBid(ctx, req)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("bid received: bidder=%q amount=%d", req.BidderId, req.Amount)
	outcome := s.state.ApplyBid(req.BidderId, req.Amount)
	if outcome != pb.Outcome_SUCCESS {
		log.Printf("bid rejected (%v), nothing to replicate", outcome)
		return &pb.BidReply{Outcome: outcome}, nil
	}

	// Replicate on EVERY success, also for a retried identical bid (same version).
	// The first attempt may have died before reaching all backups; resending the
	// full state is harmless because Restore keeps the higher version.
	s.replicateToBackups(s.state.Snapshot(), true)
	return &pb.BidReply{Outcome: outcome}, nil
}

func (s *auctionServer) Result(ctx context.Context, req *pb.ResultRequest) (*pb.ResultReply, error) {
	if !s.fd.IsPrimary() {
		return s.forwardResult(ctx, req)
	}

	// Taking the mutex means we never answer from a bid that is still being
	// replicated. If the primary crashed right after we showed such a bid, the
	// next primary could "forget" it, and a client would see the auction go backwards.
	s.mu.Lock()
	defer s.mu.Unlock()

	over, bid, who := s.state.Result()
	return &pb.ResultReply{Over: over, HighestBid: bid, HighestBidder: who}, nil
}

// ---------------------------------------------------------------------------
// Forwarding (backup -> primary)
// ---------------------------------------------------------------------------

func isForwarded(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	return ok && len(md.Get(forwardedKey)) > 0
}

// toPrimary prepares a call to the primary: a client, plus a context that
// carries the forwarding timeout and the "already forwarded" flag.
func (s *auctionServer) toPrimary(ctx context.Context) (pb.AuctionClient, context.Context, context.CancelFunc, error) {
	if isForwarded(ctx) {
		// Someone thought we were the primary, but we are not. Do not pass it on
		// again (that could loop); let the client try another node.
		return nil, nil, nil, status.Error(codes.Unavailable, "request was already forwarded and this node is not the primary")
	}
	addr := s.fd.PrimaryAddr()
	if addr == "" {
		return nil, nil, nil, status.Error(codes.Unavailable, "no primary known")
	}
	conn, err := s.conns.get(addr)
	if err != nil {
		return nil, nil, nil, status.Errorf(codes.Unavailable, "cannot reach primary %s: %v", addr, err)
	}
	fctx, cancel := context.WithTimeout(ctx, forwardTimeout)
	fctx = metadata.AppendToOutgoingContext(fctx, forwardedKey, "true")
	return pb.NewAuctionClient(conn), fctx, cancel, nil
}

func (s *auctionServer) forwardBid(ctx context.Context, req *pb.BidRequest) (*pb.BidReply, error) {
	client, fctx, cancel, err := s.toPrimary(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()

	log.Printf("forwarding bid (bidder=%q amount=%d) to primary %s", req.BidderId, req.Amount, s.fd.PrimaryAddr())
	reply, err := client.Bid(fctx, req)
	if err != nil {
		log.Printf("forwarding failed: %v", err)
		// Unavailable tells the client to try another node.
		return nil, status.Errorf(codes.Unavailable, "primary unreachable: %v", err)
	}
	return reply, nil
}

func (s *auctionServer) forwardResult(ctx context.Context, req *pb.ResultRequest) (*pb.ResultReply, error) {
	client, fctx, cancel, err := s.toPrimary(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()

	reply, err := client.Result(fctx, req)
	if err != nil {
		log.Printf("forwarding result failed: %v", err)
		return nil, status.Errorf(codes.Unavailable, "primary unreachable: %v", err)
	}
	return reply, nil
}

// ---------------------------------------------------------------------------
// Replication (primary -> backups) and catch-up
// ---------------------------------------------------------------------------

func (s *auctionServer) replicateTo(addr string, snap *pb.StateUpdate) error {
	conn, err := s.conns.get(addr)
	if err != nil {
		return err
	}
	// Deliberately NOT derived from the client's context: if the client gives up
	// halfway, we must not misread that as the backup being dead.
	ctx, cancel := context.WithTimeout(context.Background(), replicateTimeout)
	defer cancel()
	_, err = pb.NewReplicationClient(conn).Replicate(ctx, snap)
	return err
}

// replicateToBackups sends the snapshot to the alive backups ONE AT A TIME in
// ascending order (node 2 before node 3). If the primary crashes halfway, the
// lowest alive ID, which becomes the next primary, has the newest state, so
// "highest version wins" can never leave two different states with the same version.
// A backup that fails is treated as crashed (fail-stop + known time limit) and
// the bid still succeeds with the remaining nodes.
// markDead is false for catch-up pushes, where a node that has not started yet
// must not be declared dead.
func (s *auctionServer) replicateToBackups(snap *pb.StateUpdate, markDead bool) {
	backups := append([]string(nil), s.fd.AliveBackups()...)
	sort.Strings(backups)

	for _, addr := range backups {
		if err := s.replicateTo(addr, snap); err != nil {
			if markDead {
				log.Printf("replication of version %d to %s failed (%v): marking node dead", snap.Version, addr, err)
				s.fd.MarkDead(addr)
			} else {
				log.Printf("catch-up push to %s failed (%v), it will get the state on the next bid", addr, err)
			}
			continue
		}
		log.Printf("replicated version %d to %s", snap.Version, addr)
	}
}

// watchPromotion pushes our snapshot (including the deadline) to the other
// nodes whenever we become primary, and once at startup if we start as primary.
// We poll because FailureDetector has no "promoted" event.
func (s *auctionServer) watchPromotion() {
	wasPrimary := false
	for {
		isPrimary := s.fd.IsPrimary()
		if isPrimary && !wasPrimary {
			log.Printf("this node is now the primary")
			s.catchUp()
		}
		wasPrimary = isPrimary
		time.Sleep(promotionPoll)
	}
}

func (s *auctionServer) catchUp() {
	s.mu.Lock() // keep the push from interleaving with a bid
	defer s.mu.Unlock()

	snap := s.state.Snapshot()
	log.Printf("catch-up: pushing version %d to backups", snap.Version)
	s.replicateToBackups(snap, false)
}

// ---------------------------------------------------------------------------
// Node-to-node service: Replicate (Heartbeat lives in failover.go, Person 3)
// ---------------------------------------------------------------------------

type replicationServer struct {
	pb.UnimplementedReplicationServer
	state AuctionState
	id    int
	peers []string
	fd    FailureDetector
}

func newReplicationServer(state AuctionState, id int, peers []string, fd FailureDetector) *replicationServer {
	return &replicationServer{state: state, id: id, peers: peers, fd: fd}
}

// Replicate installs the primary's state. Restore ignores older versions, so
// late or duplicate updates are harmless.
func (s *replicationServer) Replicate(ctx context.Context, u *pb.StateUpdate) (*pb.Ack, error) {
	s.state.Restore(u)
	log.Printf("state replicated: version %d, highest bid %d by %q", u.Version, u.HighestBid, u.HighestBidder)
	return &pb.Ack{}, nil
}
