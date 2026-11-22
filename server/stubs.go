package main

import (
	"context"

	pb "github.com/FranmanV2/Mandatory-Activity-5/proto"
)

// ---- TEMPORARY: Person 3 replaces Heartbeat in failover.go, then deletes this file ----

func (s *replicationServer) Heartbeat(ctx context.Context, p *pb.Ping) (*pb.Pong, error) {
	return &pb.Pong{}, nil
}
