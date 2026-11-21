package main

type FailureDetector interface {
	IsPrimary() bool        // am I the primary right now?
	PrimaryAddr() string    // who should requests go to?
	AliveBackups() []string // who should the primary replicate to?
	MarkDead(addr string)   // called on replication timeout
}
