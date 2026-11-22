package main

import (
	"net"
	"strconv"
	"sync"
)

// TEMPORARY failure detector for Person 2's testing. Person 3 replaces this
// file with the real one (same newFailureDetector signature).
// Lowest ID among the nodes not marked dead is the primary. ID = port - 5000.
// Nothing here ever notices a crash by itself; only MarkDead removes a node.

const unknownID = 1 << 30

type stubDetector struct {
	id    int
	peers []string // addresses of the OTHER nodes

	mu   sync.Mutex
	dead map[string]bool
}

func newFailureDetector(id int, peers []string) FailureDetector {
	return &stubDetector{id: id, peers: peers, dead: make(map[string]bool)}
}

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

// primaryID returns the lowest ID among us and the peers not marked dead.
// Caller must hold d.mu.
func (d *stubDetector) primaryID() int {
	lowest := d.id
	for _, addr := range d.peers {
		if !d.dead[addr] && nodeID(addr) < lowest {
			lowest = nodeID(addr)
		}
	}
	return lowest
}

func (d *stubDetector) IsPrimary() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.primaryID() == d.id
}

func (d *stubDetector) PrimaryAddr() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	pid := d.primaryID()
	for _, addr := range d.peers {
		if nodeID(addr) == pid {
			return addr
		}
	}
	return "" // we are the primary ourselves
}

func (d *stubDetector) AliveBackups() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var alive []string
	for _, addr := range d.peers {
		if !d.dead[addr] {
			alive = append(alive, addr)
		}
	}
	return alive
}

func (d *stubDetector) MarkDead(addr string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dead[addr] = true
}
