package main

import (
	"context"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	pb "github.com/FranmanV2/Mandatory-Activity-5/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)



const (
	pingInterval = 500 * time.Millisecond
	pingTimeout = 1 * time.Second
	maxMissedPings = 3
)

const unknownID = 1 << 30

func nodeID(addr string) int {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return unknownID
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return unknownID
	}
	return p - 5000
}

type peer struct {
	addr string
	id   int
	seen bool

	missed int
	dead   bool
}

type detector struct {
	id    int
	peers []*peer // fixed after construction

	mu      sync.RWMutex
	primary int
}
var _ FailureDetector = (*detector)(nil)

func newFailureDetector(id int, peers []string) FailureDetector {
	d := &detector{id: id}
	for _, addr := range peers {
		d.peers = append(d.peers, &peer{addr: addr, id: nodeID(addr)})
	}

	d.mu.Lock()
	d.updatePrimaryLocked()
	d.mu.Unlock()

	for _, p := range d.peers {
		conn, err := grpc.NewClient(p.addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  200 * time.Millisecond,
					Multiplier: 1.0,
					Jitter:     0.1,
					MaxDelay:   500 * time.Millisecond,
				},
				MinConnectTimeout: pingTimeout,
			}))
		if err != nil {
			log.Fatalf("cannot create connection to peer %s: %v", p.addr, err)
		}
		go d.monitor(p, conn)
	}
	return d
}

func (d *detector) monitor(p *peer, conn *grpc.ClientConn) {
	defer conn.Close()
	client := pb.NewReplicationClient(conn)

	for !d.isDead(p) { // fail-stop: a dead peer is never pinged again
		ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
		_, err := client.Heartbeat(ctx, &pb.Ping{NodeId: int32(d.id)})
		cancel()

		if err == nil {
			d.heardFrom(p)
		} else {
			d.missedPing(p, err)
		}
		time.Sleep(pingInterval)
	}
}

func (d *detector) isDead(p *peer) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return p.dead
}

func (d *detector) heardFrom(p *peer) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if p.dead {
		return
	}
	if !p.seen {
		p.seen = true
		log.Printf("peer %s (node %d) is up: first heartbeat answered", p.addr, p.id)
	}
	if p.missed > 0 {
		log.Printf("peer %s answered again, missed counter reset", p.addr)
		p.missed = 0
	}
}

func (d *detector) missedPing(p *peer, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if p.dead || !p.seen {
		return
	}
	p.missed++
	if p.missed < maxMissedPings {
		log.Printf("peer %s missed heartbeat %d/%d (%v)", p.addr, p.missed, maxMissedPings, err)
		return
	}
	log.Printf("peer %s suspected dead (node %d): %d heartbeats missed in a row", p.addr, p.id, p.missed)
	p.dead = true
	d.updatePrimaryLocked()
}

func (d *detector) updatePrimaryLocked() {
	lowest := d.id
	for _, p := range d.peers {
		if !p.dead && p.id < lowest {
			lowest = p.id
		}
	}
	if lowest != d.primary {
		d.primary = lowest
		log.Printf("node %d is now primary", lowest)
	}
}


// IsPrimary: am I the primary right now?
func (d *detector) IsPrimary() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.primary == d.id
}

func (d *detector) PrimaryAddr() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.primary == d.id {
		return ""
	}
	for _, p := range d.peers {
		if p.id == d.primary {
			return p.addr
		}
	}
	return ""
}

func (d *detector) AliveBackups() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var alive []string
	for _, p := range d.peers {
		if p.seen && !p.dead {
			alive = append(alive, p.addr)
		}
	}
	return alive
}

func (d *detector) MarkDead(addr string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, p := range d.peers {
		if p.addr == addr && !p.dead {
			log.Printf("peer %s marked dead by replication (node %d)", p.addr, p.id)
			p.dead = true
			d.updatePrimaryLocked()
		}
	}
}

func (s *replicationServer) Heartbeat(ctx context.Context, ping *pb.Ping) (*pb.Pong, error) {
	return &pb.Pong{}, nil
}
